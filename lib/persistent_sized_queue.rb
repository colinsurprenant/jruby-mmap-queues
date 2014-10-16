# encoding: utf-8

# inspired by https://github.com/starling/starling/blob/master/lib/starling/persistent_queue.rb
# August 2014, original license is MIT style.

require "thread"
require "jruby-mmap"

class FileDriver

  def initialize(queue_file)
    @queue_file = queue_file
    reopen_log
  end

  def reopen_log
    @file = File.new(@queue_file, File::CREAT|File::RDWR)
  end

  def write(data)
    @file.write_nonblock(data)
  end

  def close
    @file.close
  end

  def rotate_log
    close
    backup_logfile = "#{@queue_file}.#{Time.now.to_i}"
    File.rename(@queue_file, backup_logfile)
    reopen_log
  end

  def size
    File.size(@queue_file)
  end

  def position
    @file.pos
  end

  def purge
    close
    File.delete(@queue_file)
  end
end

class MmapDriver

  def initialize(queue_file)
    @queue_file = queue_file
    reopen_log
  end

  def reopen_log
    @mmap = Mmap.new(@queue_file, 100 * 1024 * 1024)
  end

  def write(data)
    @mmap.put_bytes(data)
  end

  def close
    @mmap.close
  end

  def rotate_log
    close
    backup_logfile = "#{@queue_file}.#{Time.now.to_i}"
    File.rename(@queue_file, backup_logfile)
    reopen_log
  end

  def size
    File.size(@queue_file)
  end

  def position
    @mmap.position
  end

  def purge
    close
    File.delete(@queue_file)
  end
end

# TODOS
# - queue close should prevent push/pop
# - upon peristence exception should we keep on running in memory? maybe retry driver on upcoming snapshots? that would prevent raising exception on all persistence ops


class PersistentSizedQueue < Queue

  # When a log reaches the SOFT_LOG_MAX_SIZE, the Queue will wait until
  # it is empty, and will then rotate the log file.

  SOFT_LOG_MAX_SIZE = 32 * (1024**2) # 16 MB

  NL = "\n".freeze

  TRX_READ_PUSH = "PUSH".freeze
  TRX_READ_POP = "POP".freeze

  TRX_WRITE_PUSH = (TRX_READ_PUSH + NL).freeze
  TRX_WRITE_POP = (TRX_READ_POP + NL).freeze

  attr_reader :initial_bytes
  attr_reader :total_items
  attr_reader :logsize

  # def initialize(persistence_path, queue_name, max, debug = false)

  # @param [Hash] options
  # @option options [Boolean] :debug, default to false
  # @option options [Boolean] :seralize, serialize to json, default to true
  # @option options [String] :queue_file full path to persisted queue file
  # @option options [Fixnum] :max_queue_size queue maximum size
  # @option options [Object] :driver persistence driver

  def initialize(options = {})
    options = {:debug => false, :serialize => true}.merge(options)
    raise(ArgumentError, "queue size must be positive") unless options.fetch(:max_queue_size) > 0

    # persisting
    queue_file = options.fetch(:queue_file)
    @driver = options.fetch(:driver).new(queue_file)
    @logsize = @driver.size
    @total_items = 0
    @serialize = options.fetch(:serialize)

    # queue
    @que = []
    @num_waiting = 0
    @mutex = Mutex.new
    @cond = ConditionVariable.new

    # sizing
    @max_queue_size = options[:max_queue_size]
    @enque_cond = ConditionVariable.new
    @num_enqueue_waiting = 0

    super()

    # replay_transaction_log(debug)
  end

  def push(value)
    # Thread.handle_interrupt(RuntimeError => :on_blocking) do
      @mutex.synchronize do
        while true
          break if @que.length < @max_queue_size
          @num_enqueue_waiting += 1
          begin
            @enque_cond.wait @mutex
          ensure
            @num_enqueue_waiting -= 1
          end
        end

        write_wal(TRX_WRITE_PUSH + serialize(value) + NL)

        @total_items += 1
        @que.push(value)

        @cond.signal
      end
    # end
  end
  alias_method :<<, :push

  ##
  # Retrieves data from the queue.

  def pop
    rv = memory_pop
    @mutex.synchronize do
      if @que.length < @max_queue_size
        @enque_cond.signal
      end
    end

    write_wal(TRX_WRITE_POP)
    rv
  end

  def close
    @driver.close
  end

  def purge
    @driver.purge
  end

  private

  def memory_pop(non_block = false)
    # Thread.handle_interrupt(StandardError => :on_blocking) do
      @mutex.synchronize do
        while true
          return @que.shift unless @que.empty?
          raise ThreadError, "queue empty" if non_block

          begin
            @num_waiting += 1
            @cond.wait @mutex
          ensure
            @num_waiting -= 1
          end
        end
      end
    # end
  end

  # def replay_transaction_log(debug)
  #   @driver.reopen_log
  #   @logsize = @driver.size

  #   print("reading back transaction log for #{@queue_file} ") if debug

  #   while !@trx.eof?
  #     cmd = @trx.gets.chomp
  #     case cmd
  #     when TRX_READ_POP
  #       print "<" if debug
  #       @que.shift.size
  #       @total_items -= 1
  #     when TRX_READ_PUSH
  #       print ">" if debug
  #       data = @trx.gets.chomp
  #       next unless data
  #       @total_items += 1
  #       @que.push(deserialize(data))
  #     else
  #       puts("error reading transaction log, skipping invalid command '#{cmd}'") if debug
  #     end
  #   end

  #   puts(" done") if debug

  #   nil
  # end

  # @param data [String] ASCII-8BIT encoded string so that byte size is correclty computed
  def write_wal(data)
    @driver.write(data)
    @logsize += data.size
    # @driver.rotate_log if @logsize > SOFT_LOG_MAX_SIZE && self.length == 0
    # @logsize = 0
  end

  def serialize(data)
    # Marshal.dump(data)
    @serialize ? JrJackson::Raw.generate(data) : data
  end

  def deserialize(data)
    # Marshal.load(data)
    @serialize ? JrJackson::Raw.parse_raw(data) : data
  end
end