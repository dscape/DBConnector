require 'rubygems'
require 'rack'
require 'yaml'
require 'net/http'

APP_ENV   = :production
CONF_FILE = File.dirname(__FILE__) + '/conf/dbconnector.yml'

unless ARGV.size == 2
  puts "Error: Wrong number of arguments - ruby dbconnector port database is expected"
  exit 1
end

unless File.exists? CONF_FILE
   puts "Error: Invalid config file #{CONF_FILE}"
   exit 1
end

if %x(db2 connect to #{ARGV[1]}) =~ /DB21061E/
  puts "Error: Command line environment not initialized."
  exit 1
end

options = {}
options.merge!(YAML.load(File.read(CONF_FILE)))

options[:db]   = ARGV[1]
options[:port] = ARGV[0]

def log_puts str, level="INFO"
  puts "[#{Time.now.strftime "%Y-%m-%d %H:%M:%S"}] #{level.upcase}  #{str}"
end

if APP_ENV == :production
  log_stdout = File.new(File.dirname(__FILE__) + "/log/#{options[:db]}_#{Process::pid}_stdout.log", "a")
  log_stderr = File.new(File.dirname(__FILE__) + "/log/#{options[:db]}_#{Process::pid}_stderr.log", "a")
  STDOUT.reopen(log_stdout)
  STDERR.reopen(log_stderr)
end

log_puts "Database: #{options[:db]}"
log_puts "Local PID: #{Process::pid}"
log_puts "Server: #{options[:server]}"
log_puts "Host: #{options[:host]}"
log_puts "Port: #{options[:port]}\n"

app = lambda do |env|
  req = Rack::Request.new env
  res = Rack::Response.new
  case req.path_info
  when '/ping'
    res.write 'pong'
  when '/pid'
    res.write Process::pid.to_s
  when '/'
    if req.params['query'].nil? or req.params['query'].empty?
      res.write 'you have to supply the query string'
    else
      res.write %x(db2 #{req.params['query']}) 
    end
  else
    res.write 'wrong request. did you mean /ping ?'
  end
    res.finish
end

#log_puts Net::HTTP.get(options[:maestro], "/bind?key=#{options[:host]}:#{options[:port]}/options[:db]}")
eval "Rack::Handler::#{options[:server]}.run(app, {:Host => '#{options[:host]}', :Port => #{ARGV[0]}})"
#log_puts Net::HTTP.get(options[:maestro], "/unbind?key=#{options[:host]}:#{options[:port]}/options[:db]}")