if System.get_env("WARNINGS_AS_ERRORS") do
  Code.compiler_options(warnings_as_errors: true)
end

ExUnit.start()
