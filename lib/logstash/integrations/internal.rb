require 'thread'

module LogStash; module Integrations; module Internal
  require 'logstash/integrations/internal/input'
  require 'logstash/integrations/internal/output'

  include ::LogStash::Util::Loggable
  
  ADDRESS_TO_INPUT = java.util.concurrent.ConcurrentHashMap.new()
  INPUT_SENDERS = java.util.concurrent.ConcurrentHashMap.new()

  def self.addresses_by_run_state
    result = {:running => [], :not_running => []}
    ADDRESS_TO_INPUT.forEach do |address, input| 
      key = input.running? ? :running : :not_running
      result[key] << address
    end
    result
  end

  # Only really useful for tests
  def self.reset!
    ADDRESS_TO_INPUT.clear
  end

  def self.register_sender(output, addresses)
    addresses.each do |address|
      INPUT_SENDERS.compute(address) do |key, value|
        output_list = value || java.util.concurrent.ConcurrentHashMap.newKeySet()
        output_list << output
        output_list
      end
    end
  end

  def self.unregister_sender(output, addresses)
    addresses.each do |address|
      INPUT_SENDERS.computeIfPresent(address) do |key, output_list|
        output_list.delete output

        # Return nil to delete the key if possible to prevent a leak
        output_list.empty? ? nil : output_list
      end
    end
  end

  def self.send_to(address, events)
    input = ADDRESS_TO_INPUT.get(address);
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
    return ADDRESS_TO_INPUT.putIfAbsent(address, internal_input).nil?
  end

  # Return true if the input was actually listening
  def self.unlisten(address, internal_input)
    removed = false

    ADDRESS_TO_INPUT.remove(address, internal_input)
  end
end; end; end