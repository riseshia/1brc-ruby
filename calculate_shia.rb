# frozen_string_literal: true

TARGET_PATH = ARGV.first || "measurements.txt"
BUFFER_SIZE = 110

Station = Struct.new(:tmin, :tmax, :tsum, :tnum)

def main
  temp_per_station = Hash.new { |h, k| h[k] = Station.new(100.0, -101.0, 0.0, 0) }

  File.open(TARGET_PATH) do |f|
    f.each_line(BUFFER_SIZE) do |row|
      name, temp_s = row.split(";")
      temp = temp_s.to_f
      s = temp_per_station[name]
      s.tmin = temp if temp < s.tmin
      s.tmax = temp if temp > s.tmax
      s.tsum += temp
      s.tnum += 1
    end

    putc "{"
    started = false
    calculated = temp_per_station.keys.sort.map do |name|
      if started
        print ", "
      else
        started = true
      end

      s = temp_per_station[name]
      mean = s.tsum.fdiv(s.tnum).round(2).round(1)
      print format("%s=%.1f/%.1f/%.1f", name, s.tmin, mean, s.tmax)
    end
    puts "}"
  end
end

main
