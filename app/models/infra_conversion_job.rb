class InfraConversionJob < Job
  def self.create_job(options)
    # TODO: from settings/user plan settings
    options[:conversion_polling_interval] ||= Settings.transformation.limits.conversion_polling_interval # in seconds
    options[:poll_conversion_max] ||= Settings.transformation.limits.poll_conversion_max
    options[:poll_post_stage_max] ||= Settings.transformation.limits.poll_post_stage_max
    super(name, options)
  end

  # TODO: UPDATE THE DIAGRAM
  # State-transition diagram:
  #                              :poll_conversion                         :poll_post_stage
  #    *                          /-------------\                        /---------------\
  #    | :initialize              |             |                        |               |
  #    v               :start     v             |                        v               |
  # waiting_to_start --------> running ------------------------------> post_conversion --/
  #     |                         |                :start_post_stage       |
  #     | :abort_job              | :abort_job                             |
  #     \------------------------>|                                        | :finish
  #                               v                                        |
  #                             aborting --------------------------------->|
  #                                                    :finish             v
  #                                                                    finished
  #

  alias_method :initializing, :dispatch_start
  alias_method :finish,       :process_finished
  alias_method :abort_job,    :process_abort
  alias_method :cancel,       :process_cancel
  alias_method :error,        :process_error

  def load_transitions
    self.state ||= 'initialize'

    {
      :initializing   => { 'initialize'  => 'waiting_to_start' },
      :collapse_snapshots => { 'waiting_to_start' => 'collapsing_snapshots' },
      :warm_migration_sync => {
        'collapsing_snapshots' => 'warm_migration_syncing',
        'warm_migration_syncing' => 'warm_migration_syncing'
       },
      :run_pre_migration_playbook => {
        'collapsing_snapshots' => 'running_pre_migration_playbook',
        'warm_migration_syncing' => 'running_pre_migration_playbook',
        'running_pre_migration_playbook' => 'running_pre_migration_playbook'
      },
      'shutdown' => {
        'running_pre_migration_playbook' => 'shutting_down',
        'shutting_down' => 'shutting_down'
      },
      'cold_migration_convert' => {
        'shutting_down' => 'cold_migration_syncing',
        'cold_migration_syncing' => 'cold_migration_syncing'
      },
      'warm_migration_finalize' => {
        'shutting_down' => 'warm_migration_finalizing',
        'warm_migration_finalizing' => 'warm_migration_finalizing'
      },
      'restore_attributes' => {
        'cold_migration_syncing' => 'restoring_attributes',
        'warm_migration_finalizing' => 'restoring_attributes'
      },
      'apply_right_size' => { 'restoring_attributes' => 'applying_right_size' },
      'power_on' => { 'applying_right_size' => 'powering_on' },
      'run_post_migration_playbook' => { 'powering_on' => 'running_post_migration_playbook' }
      'finish' => { '*' => 'finished' },
      'abort_job' => { '*' => 'aborting' },
      'cancel' => { '*' => 'canceling' },
      'error' => { '*' => '*' }
    }
  end

  def migration_task
    @migration_task ||= target_entity
    # valid states: %w(migrated pending finished active queued)
  end

  def warm_migration?
    migration_task.options[:warm_migration_requested]
  end

  def vm
    @vm || migration_task.source
  end

  def initializing
    migration_task.preflight_check
    _log.info(prep_message("Preflight check passed, task.state=#{migration_task.state}. continue ..."))
    queue_signal(:start)
  rescue => error
    message = prep_message("Preflight check has failed: #{message}")
    _log.info(message)
    abort_conversion(message, 'error')
  end

  def start
    _log.info(prep_message("ght check passed, task.state=#{migration_task.state}. continue ..."))
    queue_signal(:poll_conversion)
  rescue => error
    message = prep_message("Preflight check has failed: #{error}")
    _log.info(message)
    abort_conversion(message, 'error')
  end

  def collapse_snapshots
    vm.remove_all_napshots unless vm.vendor != 'vmware' or vm.snapshots.empty?
    signal =  warm_migration? ? :warm_migration_sync : :run_pre_migration_playbook
    queue_signal(signal)
  end

  def warm_migration_sync
    return abort_conversion('Warm migration sync timed out', 'error') if polling_timeout(:warm_migration_sync)

    # If virt-v2v-wrapper has not been launched yet, then launch it
    unless migration_task.options[:virtv2v_wrapper_started_on]
      message = 'Virt-v2v-wrapper is not started. Starting it.'
      _log.info(prep_message(message))
      migration_task.run_conversion(:warm)
      return queue_signal(:warm_migration_sync, :deliver_on => Time.now.utc + options[:warm_migration_sync_polling_interval]
    end

    # If virt-v2v-wrapper has been launch, but has not created its state file yet, then retry
    unless migration_task.options.fetch_path(:virtv2v_wrapper, 'state_file')
      message = 'Virt-v2v-wrapper state file not available, continuing warm_migration_sync'
      _log.info(prep_message(message))
      update_attributes(:message => message)
      return queue_signal(:warm_migration_sync, :deliver_on => Time.now.utc + options[:warm_migration_sync_polling_interval]
    end

    # Retrieve the conversion state
    begin
      migration_task.get_conversion_state
    rescue ==> error
      _log.log_backtrace(error)
      return abort_conversion("Warm migration sync error: #{error.message}", 'error')
    end

    # Logging and displaying the status of warm migration
    warm_migration_sync_status = migration_task.options[:warm_migration_sync_status]
    message = "warm_migration_sync_status=#{warm_migration_sync_status}"
    _log.info(prep_message(message)
    update_attributes(:message => message)

    # Update message if warm migration sync has failed
    if warm_migration_sync_status == 'failed'
      message = migration_task.options[:warm_migration_sync_message]
      return abort_conversion(prep_message("Warm migration sync failed: #{message}"))
    end

    # If the cutover date/time has been reached, move to the next state
    if migration_task.optons[:warm_migration_finalize_datetime] < Time.now.utc
      _log.info("Warm migration finalization time has been reached, continuing migration process")
      return queue_signal(:run_pre_migration_playbook)
    end

    # Otherwise, retry
    queue_signal(:warm_migration_sync, :deliver_on => Time.now.utc + options[:warm_migration_sync_polling_interval]
  end

  def run_pre_migration_playbook
    service_request_id = migration_task.options[:pre_ansible_playbook_service_request_id]

    if service_request_id.nil?
      return queue_signal(:shutdown) if vm.ipaddresses.empty?
      service_template = migration_task.pre_ansible_playbook_service_template
      service_dialog_options = { :hosts => vm.ipaddresses.first }
      service_request = create_service_provision_request(service_template, service_dialog_options) # TODO
      migration_task.options[:pre_ansible_playbook_service_request_id] = service_request.id
      return queue_signal(:run_pre_migration_playbook, :deliver_on => Time.now.utc + options[:migration_playbook_polling_interval])
    end

    service_request = MiqRequest.find_by(service_request_id)

    playbook_status = migration_task.options[:pre_migration_playbook_status] || {}
    playbook_status[:job_state] = service_request.state
    playbook_status[:job_status] = service_request.status
    playbooks_status[:job_id] ||= service_request.miq_request_tasks.first.destination.service_resources.first.resource.id
    migration_task.options[:pre_migration_playbook_status] = playbook_status

    return abort_conversion('Premigration playbook failed', 'error') if playbook_status[:job_status] == 'Error'
    return queue_signal(:shutdown) if playbook_status[:job_state] == 'finished'
    queue_signal(:run_pre_migration_playbook, :deliver_on => Time.now.utc + options[:migration_playbook_polling_interval]
  end

  def shutdown
    if vm.power_state == 'off'
      signal = warm_migration? ? :warm_migration_finalize : :cold_migration_convert
      return queue_signal(signal)
    end

    if polling_timeout(:shutdown)
      vm.stop
    elsif migration_task.options[:shutdown_requested_time].nil?
      vm.shutdown_guest
      migration_task.options[:shutdown_requested_time] = Time.now.utc
    end

    queue_signal(:shutdown, :deliver_on => Time.now.utc + options[:shutdown_polling_interval]
  end

  def warm_migration_finalize
    return abort_conversion('Warm migration finalize timed out', 'error') if polling_timeout(:warm_migration_finalize)

    unless migration_task.options[:warm_migration_finalize_started_on]
      migration_task.finalize_warm_migration
      return queue_signal(:warm_migration_finalize, :deliver_on => Time.now.utc + options[:warm_migration_finalize_polling_internal]
    end
    
    # Retrieve the conversion state
    begin
      migration_task.get_conversion_state
    rescue ==> error
      _log.log_backtrace(error)
      return abort_conversion("Warm migration sync error: #{error.message}", 'error')
    end

    # Logging and displaying the status of warm migration
    warm_migration_finalize_status = migration_task.options[:warm_migration_finalize_status]
    message = "warm_migration_finalize_status=#{warm_migration_finalize_status}"
    _log.info(prep_message(message)
    update_attributes(:message => message)

    # Update message if warm migration sync has failed
    if warm_migration_finalize_status == 'failed'
      message = migration_task.options[:warm_migration_finalize_message]
      return abort_conversion(prep_message("Warm migration finalize failed: #{message}"))
    end

    # Otherwise, retry
    queue_signal(:warm_migration_finaize, :deliver_on => Time.now.utc + options[:warm_migration_finalize_polling_interval]
  end

  def abort_conversion(message, status)
    # TransformationCleanup 3 things:
    #  - kill v2v: ignored because no converion_host is there yet in the original automate-based logic
    #  - power_on: ignored
    #  - check_power_on: ignore
    migration_task.cancel
    queue_signal(:abort_job, message, status)
  end

  def polling_timeout(poll_type)
    count = "#{poll_type}_count".to_sym
    max = "#{poll_type}_max".to_sym
    context[count] = (context[count] || 0) + 1
    context[count] > options[max]
  end

  def cold_migration_convert
    return abort_conversion("Polling times out", 'error') if polling_timeout(:poll_conversion)

    message = "Getting conversion state"
    _log.info(prep_message(message))

    unless migration_task.options.fetch_path(:virtv2v_wrapper, 'state_file')
      message = "Virt v2v state file not available, continuing poll_conversion"
      _log.info(prep_message(message))
      update_attributes(:message => message)
      return queue_signal(:cold_migration_convert, :deliver_on => Time.now.utc + options[:conversion_polling_interval])
    end

    begin
      migration_task.get_conversion_state # migration_task.options will be updated
    rescue => exception
      _log.log_backtrace(exception)
      return abort_conversion("Conversion error: #{exception}", 'error')
    end

    v2v_status = migration_task.options[:virtv2v_status]
    message = "virtv2v_status=#{v2v_status}"
    _log.info(prep_message(message))
    update_attributes(:message => message)

    case v2v_status
    when 'active'
      queue_signal(:cold_migration_convert, :deliver_on => Time.now.utc + options[:conversion_polling_interval])
    when 'failed'
      message = "disk conversion failed"
      abort_conversion(prep_message(message), 'error')
    when 'succeeded'
      message = "disk conversion succeeded"
      _log.info(prep_message(message))
      queue_signal(:start_post_stage)
    else
      message = prep_message("Unknown converstion status: #{v2v_status}")
      abort_conversion(message, 'error')
    end
  end

  def start_post_stage
    # once we refactor Automate's PostTransformation into a job, we kick start it here
    message = "To wait for Post-Transformation progress"
    _log.info(prep_message(message))
    update_attributes(:message => message)
    queue_signal(:poll_post_stage, :deliver_on => Time.now.utc + options[:conversion_polling_interval])
  end

  def poll_post_stage
    return abort_conversion("Polling times out", 'error') if polling_timeout(:poll_post_stage)

    message = "PostTransformation state=#{migration_task.state}, status=#{migration_task.status}"
    _log.info(prep_message(message))
    update_attributes(:message => message)
    if migration_task.state == 'finished'
      self.status = migration_task.status
      queue_signal(:finish)
    else
      queue_signal(:poll_post_stage, :deliver_on => Time.now.utc + options[:conversion_polling_interval])
    end
  end

  def queue_signal(*args, deliver_on: nil)
    MiqQueue.put(
      :class_name  => self.class.name,
      :method_name => "signal",
      :instance_id => id,
      :role        => "ems_operations",
      :zone        => zone,
      :task_id     => guid,
      :args        => args,
      :deliver_on  => deliver_on
    )
  end

  def prep_message(contents)
    "MiqRequestTask id=#{migration_task.id}, InfraConversionJob id=#{id}. #{contents}"
  end
end
