class ::LogStash::Integrations::Internal::Input < ::LogStash::Inputs::Base
  config_name "internal"

  config :address, :validate => :string, :required => true

  def register
    # May as well set this up here, writers won't do anything until
    # @running is set to false
    @running = java.util.concurrent.atomic.AtomicBoolean.new(false)
    listen_successful = ::LogStash::Integrations::Internal.listen(@address, self)
    if !listen_successful
      raise ::LogStash::ConfigurationError, "Internal input at '#{@address}' already bound! Addresses must be globally unique across pipelines."
    end
  end

  def run(queue)
    @queue = queue
    @running.set(true)

    while @running.get()
      sleep 0.5
    end
  end

  def running?
    @running && @running.get()
  end

  # Returns false if the receive failed due to a stopping input
  # To understand why this value is useful see Internal.send_to
  def internal_receive(events)
    return false if !@running.get()

    events.each do |e| 
      clone = e.clone
      decorate(clone)
      @queue << clone
    end

    return true
  end

  def stop
    # We stop receiving events before we unlisten to prevent races
    @running.set(false) if @running # If register wasn't yet called, no @running!
    ::LogStash::Integrations::Internal.unlisten(@address, self)
  end
end