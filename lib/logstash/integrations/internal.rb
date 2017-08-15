module LogStash; module Integrations; module Internal
  include ::LogStash::Util::Loggable
  def self.start!
    @inputs = java.util.concurrent.ConcurrentHashMap.new
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
    return input && input.internal_receive(events)
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
      # nothing to do, but this is required by the plugin API
      @running = java.util.concurrent.atomic.AtomicBoolean.new(true)
    end

    def run(queue)
      @queue = queue
      Internal.listen(@address, self)

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

    config_name "internal"

    config :send_to, :validate => :string, :required => true, :list => true

    def register
    end

    def multi_receive(events)
      @send_to.each do |address|
        while !Internal.send_to(address, events)
          sleep 1
          @logger.error("Internal output to address '#{address}' blocked, no inputs have this address!")
        end
      end
    end
  end
end; end; end