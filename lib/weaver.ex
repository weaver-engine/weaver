defmodule Weaver do
  @moduledoc """
  Root module and main API of Weaver.
  """

  alias Weaver.{GraphQL, Step}
  alias Weaver.GraphQL.Resolver

  defmodule Ref do
    @moduledoc """
    References a node in the graph using a globally unique `id`.

    Used as placeholder in any graph tuples, such as for storing
    and retrieval in `Weaver.Graph`.
    """

    @enforce_keys [:id]
    defstruct @enforce_keys

    @type t() :: %__MODULE__{
            id: String.t()
          }

    def new(id), do: %__MODULE__{id: id}
    def from(obj), do: new(Weaver.Resolvers.id_for(obj))
  end

  defmodule Cursor do
    @moduledoc """
    References a position in a timeline.

    Can be stored as part of a marker in the meta data
    together with the actual graph data in `Weaver.Graph`.
    """

    @enforce_keys [:ref, :val]
    defstruct @enforce_keys ++ [:gap]

    @type t() :: %__MODULE__{
            ref: any(),
            val: any()
          }

    def new(ref, val, gap \\ nil)

    def new(ref, val, true) do
      Weaver.Marker.ChunkEnd.new(ref, val)
    end

    def new(ref, val, _gap) do
      Weaver.Marker.ChunkStart.new(ref, val)
    end
  end

  defmodule Marker do
    @moduledoc """
    Namespace for markers in a timeline that can be stored as
    meta data together with the actual graph data in `Weaver.Graph`.
    """

    defmodule ChunkStart do
      @moduledoc """
      References a position in a timeline where a chunk starts,
      used as boundary for retrieval of new records in the timeline.

      Can be stored as meta data together with the actual graph
      data in `Weaver.Graph`.
      """

      @enforce_keys [:cursor]
      defstruct @enforce_keys

      @type t() :: %__MODULE__{cursor: Weaver.Cursor.t()}

      def new(id, val) do
        %__MODULE__{cursor: %Weaver.Cursor{ref: %Ref{id: id}, val: val}}
      end
    end

    defmodule ChunkEnd do
      @moduledoc """
      References a position in a timeline used for resuming the
      retrieval at a previous point if needed.

      Can be stored as meta data together with the actual graph
      data in `Weaver.Graph`.
      """

      @enforce_keys [:cursor]
      defstruct @enforce_keys

      @type t() :: %__MODULE__{cursor: Weaver.Cursor.t()}

      def new(id, val) do
        %__MODULE__{cursor: %Weaver.Cursor{ref: %Ref{id: id}, val: val}}
      end
    end
  end

  def prepare(query, opts \\ []) do
    {ast, fun_env} = parse_query(query)

    %Step{
      ast: ast,
      fun_env: fun_env,
      cache: Keyword.get(opts, :cache),
      operation: Keyword.get(opts, :operation, ""),
      variables: Keyword.get(opts, :variables, %{})
    }
  end

  def weave(query, opts \\ [])

  def weave(query, opts) when is_binary(query) do
    prepare(query, opts)
    |> weave()
  end

  def weave(step = %Step{}, _opts) do
    Step.process(step)
  end

  def parse_query(query) do
    with {:ok, ast} <- :graphql.parse(query),
         {:ok, %{ast: ast, fun_env: fun_env}} <- :graphql.type_check(ast),
         :ok <- :graphql.validate(ast) do
      {ast, fun_env}
    end
  end

  def load_schema() do
    with :ok <- :graphql.load_schema(mapping(), schema()),
         :ok <- :graphql.insert_schema_definition(root_schema()),
         :ok <- :graphql.validate_schema() do
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
