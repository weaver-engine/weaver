defmodule Weaver.Absinthe.Schema do
  @moduledoc """
  GraphQL schema for testing fetching data from Twitter.
  """
  use Absinthe.Schema

  alias ExTwitter.Model.{Tweet, User}
  alias Weaver.Resolvers

  defimpl Weaver.Node, for: Tweet do
    def id_for(tweet), do: "Tweet:#{tweet.id_str}"
  end

  defimpl Weaver.Node, for: User do
    def id_for(user), do: "TwitterUser:#{user.screen_name}"
  end

  interface :node do
    field(:id, non_null(:id))
  end

  object :twitter_user do
    interface(:node)

    is_type_of(fn
      %User{} -> true
      _other -> false
    end)

    field(:id, non_null(:id), resolve: &weaver_id/3)
    field(:screen_name, non_null(:string))
    field(:favorites_count, non_null(:integer), resolve: rsv(:favourites_count))
    field(:favorites, non_null(list_of(non_null(:tweet))), resolve: dispatched("favorites"))
    field(:tweets, non_null(list_of(non_null(:tweet))), resolve: dispatched("tweets"))
    field(:retweets, non_null(list_of(non_null(:tweet))), resolve: dispatched("retweets"))
  end

  object :tweet do
    interface(:node)

    is_type_of(fn
      %Tweet{} -> true
      _other -> false
    end)

    field(:id, non_null(:id), resolve: &weaver_id/3)
    field(:text, non_null(:string), resolve: rsv(:full_text))
    field(:published_at, non_null(:string), resolve: rsv(:created_at))
    field(:likes_count, non_null(:integer), resolve: rsv(:favorite_count))
    field(:retweets_count, non_null(:integer), resolve: rsv(:retweet_count))
    field(:user, non_null(:twitter_user))
    field(:likes, non_null(list_of(non_null(:twitter_like))), resolve: dispatched("likes"))
    field(:mentions, non_null(list_of(non_null(:twitter_user))), resolve: dispatched("mentions"))
    field(:retweets, non_null(list_of(non_null(:tweet))), resolve: dispatched("retweets"))
    field(:replies, non_null(list_of(non_null(:tweet))), resolve: dispatched("replies"))
    field(:retweet_of, :tweet, resolve: rsv(:retweeted_status))
  end

  object :twitter_like do
    field(:user, :twitter_user)
  end

  object :mention do
    field(:user, :twitter_user)
  end

  query do
    field :node, non_null(:node) do
      arg(:id, :string)

      resolve(fn _, %{id: id}, _ ->
        obj = Resolvers.retrieve_by_id(id)
        {:ok, obj}
      end)
    end
  end

  def middleware(middleware, _field, _) do
    middleware
    |> Absinthe.Pipeline.without(Absinthe.Middleware.Telemetry)
  end

  defp dispatched(field) do
    fn obj, _, _ ->
      fun = fn prev_end_marker ->
        Resolvers.dispatched(obj, field, prev_end_marker)
      end

      {:middleware, Weaver.Absinthe.Middleware.Dispatch, fun}
    end
  end

  defp rsv(field) when is_atom(field) do
    fn obj, _, _ -> Map.fetch(obj, field) end
  end

  defp weaver_id(obj, _, _) do
    {:ok, Weaver.Node.id_for(obj)}
  end
end
