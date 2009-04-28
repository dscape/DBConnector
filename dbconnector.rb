require 'rubygems'
require 'vendor/amqp/lib/mq'
require 'yaml'

APP_ENV   = :development
CONF_FILE = File.dirname(__FILE__) + '/conf/dbconnector.yml'

def log_puts str, level="INFO"
  puts "[#{Time.now.strftime "%Y-%m-%d %H:%M:%S"}] #{level.upcase}  #{str}"
end

unless File.exists? CONF_FILE
   puts "Error: Invalid config file #{CONF_FILE}"
   exit 1
end

options = {}
options.merge!(YAML.load(File.read(CONF_FILE)))

if ARGV.size == 1
  name = ARGV[0]

  if %x(db2 connect to #{name}) =~ /DB21061E/
    puts "Error: Command line environment not initialized."
    exit 1
  end
else
  name = '_none'
end


if APP_ENV == :production
  log_stdout = File.new(File.dirname(__FILE__) + "/log/#{name}_#{Process::pid}_stdout.log", "a")
  log_stderr = File.new(File.dirname(__FILE__) + "/log/#{name}_#{Process::pid}_stderr.log", "a")
  STDOUT.reopen(log_stdout)
  STDERR.reopen(log_stderr)
end

log_puts "MQ: #{options[:host]}:#{options[:port]}"
log_puts "Database: #{name}"
log_puts "Local PID: #{Process::pid}\n"

AMQP.start(:host => 'localhost') do
  amq = MQ.new
  amq.queue(name).subscribe do |msg|
    result = %x(db2 #{msg})
    log_puts "Processed #{msg}"
    amq.queue("#{name}_response").publish(result)
  end
end