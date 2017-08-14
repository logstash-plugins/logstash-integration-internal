require 'logstash/integrations/internal'

LogStash::Integrations::Internal.start!

LogStash::PLUGIN_REGISTRY.add(:input, "internal", LogStash::Integrations::Internal::Input)
LogStash::PLUGIN_REGISTRY.add(:output, "internal", LogStash::Integrations::Internal::Output)