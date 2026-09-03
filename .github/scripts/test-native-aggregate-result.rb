#!/usr/bin/env ruby
# frozen_string_literal: true

require 'open3'
require_relative 'verify-merge-gates'

template = MergeGateVerifier.aggregate_run_script(['postgres'])

%w[failure cancelled skipped].each do |result|
  script = template.sub('${{ needs.postgres.result }}', result)
  _stdout, _stderr, status = Open3.capture3('bash', '-c', script)
  abort "aggregate unexpectedly accepted #{result}" if status.success?
end

script = template.sub('${{ needs.postgres.result }}', 'success')
_stdout, stderr, status = Open3.capture3('bash', '-c', script)
abort "aggregate rejected success: #{stderr}" unless status.success?

puts 'native aggregate accepts only successful dependency results'
