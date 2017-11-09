require 'logstash/integrations/internal'

LogStash::PLUGIN_REGISTRY.add(:input, "internal", LogStash::Integrations::Internal::Input)
LogStash::PLUGIN_REGISTRY.add(:output, "internal", LogStash::Integrations::Internal::Output)