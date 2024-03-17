# frozen_string_literal: true

TARGET_PATH = ARGV.first || "measurements.txt"
FILE_READ_BUFFER_SIZE = 110
PARSER_NUM = 16
BULK_ENQUEUE_SIZE = 10_000

Row = Data.define(:name, :temperature)
Result = Data.define(:min, :max, :mean)

def make_queue
  Ractor.new do
    loop do
      Ractor.yield(Ractor.receive)
    end
  end
end

def make_parser(passed_queue, passed_router)
  Ractor.new(passed_queue, passed_router) do |queue, router|
    loop do
      row_strs = queue.take
      break if row_strs.nil? # go fin process if passed nil

      rows = row_strs.map do |row_str|
        name, temperature = row_str.split(";")

        Row.new(name:, temperature: temperature.to_f)
      end
      router.send(rows)
    end
    # puts "fin parser"
  end
end

def make_merger
  Ractor.new do
    min = 101.0
    max = -100.0
    sum = 0.0
    count = 0

    loop do
      row = Ractor.receive

      break if row.nil? # go fin process if passed nil

      count += 1
      sum += row.temperature
      min = row.temperature if min > row.temperature
      max = row.temperature if max < row.temperature
    end

    Result.new(min:, max:, mean: sum.fdiv(count).round(2).round(1))
  end
end

def make_router
  Ractor.new do
    merger_per_name = {}

    loop do
      rows = Ractor.receive

      break if rows.nil? # go fin process if passed nil

      rows.each do |row|
        merger_per_name[row.name] ||= make_merger
        merger = merger_per_name[row.name]
        merger.send(row)
      end
    end

    merger_per_name.each_value { |r| r.send(nil) } # send fin signal

    merger_per_name
  end
end

def main
  queue_ractor = make_queue
  router_ractor = make_router
  parser_ractors = PARSER_NUM.times.map { make_parser(queue_ractor, router_ractor) }

  File.open(TARGET_PATH) do |f|
    buffer = []
    f.each_line(FILE_READ_BUFFER_SIZE, chomp: true) do |row|
      buffer << row

      if buffer.size > BULK_ENQUEUE_SIZE
        queue_ractor.send(buffer)
        buffer = []
      end
    end
    queue_ractor.send(buffer) if !buffer.empty?
  end

  # ensure parser fin
  PARSER_NUM.times { queue_ractor.send(nil) }
  parser_ractors.each { |r| r.take }

  # ensure router fin
  router_ractor.send(nil)

  results_per_name = router_ractor.take

  calculated = results_per_name.keys.sort.map do |name|
    result = results_per_name[name].take

    min = result.min
    max = result.max
    mean = result.mean
    format("%s=%.1f/%.1f/%.1f", name, min, mean, max)
  end

  puts "{#{calculated.join(', ')}}"
end

main
