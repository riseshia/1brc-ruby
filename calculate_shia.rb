# frozen_string_literal: true

TARGET_PATH = ARGV.first || "measurements.txt"
BUFFER_SIZE = 110

def main
  temp_per_station = Hash.new { |h, k| h[k] = [] }

  File.open(TARGET_PATH) do |f|
    f.each_line(BUFFER_SIZE) do |row|
      name, temp = row.split(";")
      temp_per_station[name] << temp.to_f
    end

    calculated = temp_per_station.keys.sort.map do |name|
      min = temp_per_station[name].min
      max = temp_per_station[name].max
      mean = temp_per_station[name].sum.fdiv(temp_per_station[name].size).round(1)
      format("%s=%.1f/%.1f/%.1f", name, min, mean, max)
    end

    puts "{#{calculated.join(', ')}}"
  end
end

main
