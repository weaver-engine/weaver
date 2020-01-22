defmodule Weaver do
  alias Weaver.GraphQL
  alias Weaver.GraphQL.Resolver

  defmodule Tree do
    @enforce_keys [
      :ast
    ]

    defstruct @enforce_keys ++
                [
                  :callback,
                  :source_graph,
                  :data,
                  :uid,
                  :fun_env,
                  :operation,
                  :variables,
                  :cursor,
                  refresh: true,
                  backfill: true,
                  refreshed: false,
                  gap: :not_loaded,
                  count: 0
                ]
  end

  defmodule Ref do
    @enforce_keys [:id]
    defstruct @enforce_keys

    def new(id), do: %__MODULE__{id: id}
  end

  defmodule Cursor do
    @enforce_keys [:ref, :val]
    defstruct @enforce_keys ++ [:gap]

    def new(ref, val, gap \\ nil) do
      %__MODULE__{ref: ref, val: val, gap: gap}
    end
  end

  def parse_query(query) do
    with {:ok, ast} <- :graphql.parse(query),
         {:ok, %{ast: ast, fun_env: fun_env}} <- :graphql.type_check(ast),
         :ok <- :graphql.validate(ast) do
      {ast, fun_env}
    end
  end

  def query(query, operation \\ "", variables \\ %{}) when is_map(variables) do
    with {ast, fun_env} <- parse_query(query) do
      coerced = :graphql.type_check_params(fun_env, operation, variables)
      context = %{params: coerced, operation_name: operation}
      result = :graphql.execute(context, ast)

      %GraphQL.Query{
        query: query,
        operation: operation,
        params: coerced,
        environment: fun_env,
        result: struct(GraphQL.Query.Result, result)
      }
    else
      # todo, handle errors
      _ -> nil
    end
  end

  def load_schema() do
    with :ok = :graphql.load_schema(mapping(), schema()),
         :ok = :graphql.insert_schema_definition(root_schema()),
         :ok = :graphql.validate_schema() do
      :ok
    end
  end

  defp schema() do
    [File.cwd!(), "priv", "weaver", "schema.graphql"]
    |> Path.join()
    |> File.read!()
  end

  defp root_schema() do
    {:root,
     %{
       :query => "Query",
       :mutation => "Mutation",
       :interfaces => ["Node"]
     }}
  end

  defp mapping() do
    %{
      scalars: %{default: GraphQL.Scalar.Default},
      interfaces: %{default: GraphQL.Interface.Default},
      unions: %{default: GraphQL.Union.Default},
      enums: %{default: GraphQL.Enum.Default},
      objects: %{
        Query: Resolver.QueryRoot,
        Mutation: Resolver.Mutation,
        TwitterUser: Weaver.Twitter.User,
        Tweet: Weaver.Twitter.Tweet,
        default: Resolver.Default
      }
    }
  end
end
