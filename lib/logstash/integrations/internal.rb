module LogStash; module Integrations; module Internal
  include ::LogStash::Util::Loggable
  def self.start!
    @subscribers = java.util.concurrent.ConcurrentHashMap.new
  end

  def self.send_to(address, events)
    subscribers = @subscribers.get(address);
    if subscribers && !subscribers.empty?
      subscribers.each do |subscriber|
        subscriber.internal_receive(events)
      end

      return true
    else
      return false
    end
  end

  def self.subscribe_to(address, internal_input)
    address_subscribers = @subscribers.computeIfAbsent(address) do
      java.util.concurrent.ConcurrentHashMap.newKeySet()
    end

    address_subscribers.add(internal_input)
  end

  def self.unsubscribe_from(address, internal_input)
    @subscribers.compute(address) do |k,address_subscribers|
      next nil unless address_subscribers
      address_subscribers.remove(internal_input)
    end
  end

  class Input < ::LogStash::Inputs::Base
    config_name "internal"

    config :address, :validate => :string, :required => true

    def register
      # nothing to do, but this is required by the plugin API
    end

    def run(queue)
      @queue = queue
      Internal.subscribe_to(@address, self)

      while !@stopping
        sleep 0.1
      end
    end

    def internal_receive(events)
      events.each do |e| 
        clone = e.clone
        decorate(clone)
        @queue << clone
      end
    end

    def stop
      Internal.unsubscribe_from(@address, self)
      @stopping = true
    end
  end

  class Output < ::LogStash::Outputs::Base
    config_name "internal"

    config_name "internal"

    config :address, :validate => :string, :required => true

    def register
    end

    def multi_receive(events)
      while !Internal.send_to(@address, events)
        sleep 1
        @logger.error("Internal output to address '#{@address}' blocked, no inputs are subscribed to this address!")
      end
    end
  end
end; end; end