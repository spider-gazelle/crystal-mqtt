require "./spec_helper"

# Requiring an entry point directly must work.
#
# `spec_helper` requires `src/mqtt` first, which loads the whole shard and hides
# any ordering problem from every other spec. A consumer does not do that — it
# requires one of the documented entry points and nothing else. Shipping 2.0.0
# broke three of the four this way, including `mqtt/v3/client`, which is what
# every 1.x user has.
#
# So this compiles each entry point on its own, the way a consumer sees it.
describe "require entry points" do
  {
    "src/mqtt"           => "MQTT::Client",
    "src/mqtt/client"    => "MQTT::Client",
    "src/mqtt/v5/client" => "MQTT::V5::Client",
    "src/mqtt/v3/client" => "MQTT::V3::Client",
  }.each do |entry, constant|
    it "compiles with only `require \"#{entry}\"`" do
      root = File.expand_path("..", __DIR__)
      # written inside the project so shard requires resolve against `lib/`
      path = File.join(root, ".entry_point_check.cr")
      File.write(path, %(require "./#{entry}"\nputs #{constant}\n))

      begin
        output = IO::Memory.new
        status = Process.run(
          "crystal",
          ["build", "--no-codegen", ".entry_point_check.cr"],
          output: output,
          error: output,
          chdir: root
        )
        fail "require \"#{entry}\" does not compile on its own:\n#{output}" unless status.success?
      ensure
        File.delete?(path)
      end
    end
  end
end
