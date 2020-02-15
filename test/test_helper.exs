if System.get_env("WARNINGS_AS_ERRORS") do
  Code.compiler_options(warnings_as_errors: true)
end

Weaver.load_schema()

{:ok, _pid} = Weaver.Loom.start_link(nil)
{:ok, _pid} = Weaver.Graph.start_link(nil)

{:ok, _pid} =
  Application.get_env(:weaver, :dgraph, [])
  |> Keyword.merge(name: Dlex)
  |> Dlex.start_link()

ExUnit.start()
