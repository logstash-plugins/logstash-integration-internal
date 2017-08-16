module LogStash; module Integrations; module Internal
  include ::LogStash::Util::Loggable

  def self.start!
    @inputs = java.util.concurrent.ConcurrentHashMap.new
  end

  def self.addresses_by_run_state
    result = {:running => [], :not_running => {}}
    @inputs.forEach do |address, input| 
      key = input.running? ? :running : :not_running
      result[key] << address
    end
    result
  end

  def self.send_to(address, events)
    input = @inputs.get(address);
    # Internal receive returns a boolean indicating whether the receive was successful or not
    # If the result is false then the sender will retry.
    # If this were not here we'd have a race where an input could be stopped after the CHM.get above
    # but before this line.
    # You might think this would be solvable in a simpler way using CHM.compute {|address, input| input.internal_receive(events) }
    # but that would be problematic since `internal_receive` blocks indefinitely on pipeline backpressure
    # this is much more dependable
    if input
      return input.internal_receive(events)
    end
  end

  # Return true if nothing was listening previously
  def self.listen(address, internal_input)
    mapped_input = @inputs.putIfAbsent(address, internal_input).nil?
    return mapped_input == internal_input
  end

  # Return true if the input was actually listening
  def self.unlisten(address, internal_input)
    @inputs.remove(address, internal_input)
  end

  class Input < ::LogStash::Inputs::Base
    config_name "internal"

    config :address, :validate => :string, :required => true

    def register
      @running = java.util.concurrent.atomic.AtomicBoolean.new(false)
    end

    def running?
      @running.get()
    end

    def run(queue)
      @queue = queue
      Internal.listen(@address, self)

      # Now that the listener is set up we can activate this
      @running.set(true)

      while @running.get()
        sleep 0.5
      end
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
      @running.set(false)
      Internal.unlisten(@address, self)
    end
  end

  class Output < ::LogStash::Outputs::Base
    config_name "internal"

    config :send_to, :validate => :string, :required => true, :list => true

    def register
      # Noop, needed to conform to plugin API
    end

    def multi_receive(events)
      @send_to.each do |address|
        while !Internal.send_to(address, events)
          sleep 1
          @logger.info("Internal output to address waiting for listener to start",
            :destination_address => address,
            :registered_addresses => Internal.addresses_by_run_state)
        end
      end
    end
  end
end; end; end