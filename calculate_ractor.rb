# frozen_string_literal: true

require "csv"

TARGET_PATH = "measurements.txt"

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
      row_str = queue.take
      break if row_str.nil? # go fin process if passed nil

      name, temperature = row_str.split(";")

      row = Row.new(name:, temperature: temperature.to_f)
      router.send(row)
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
      row = Ractor.receive

      break if row.nil? # go fin process if passed nil

      merger_per_name[row.name] ||= make_merger
      merger = merger_per_name[row.name]

      merger.send(row)
    end

    merger_per_name.each_value { |r| r.send(nil) } # send fin signal

    merger_per_name
  end
end

def main
  queue_ractor = make_queue
  router_ractor = make_router
  parser_num = 8
  parser_ractors = parser_num.times.map { make_parser(queue_ractor, router_ractor) }

  CSV.foreach(TARGET_PATH) do |row|
    queue_ractor.send(row[0], move: true)
  end

  # ensure parser fin
  parser_num.times { queue_ractor.send(nil) }
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
