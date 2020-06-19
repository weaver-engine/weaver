defmodule Weaver.Absinthe.Schema do
  @moduledoc """
  GraphQL schema for testing fetching data from Twitter.
  """
  use Absinthe.Schema

  alias ExTwitter.Model.{Tweet, User}
  alias Weaver.Node

  @twitter_client Application.get_env(:weaver, :twitter)[:client_module]
  @api_count Application.get_env(:weaver, :twitter)[:api_count]
  @api_take Application.get_env(:weaver, :twitter)[:api_take]

  defimpl Node, for: Tweet do
    def id_for(tweet), do: tweet.id_str
  end

  defimpl Node, for: User do
    def id_for(user), do: user.screen_name
  end

  interface :node do
    field(:id, non_null(:id))
  end

  object :twitter_user do
    interface(:node)

    meta(weaver_id: :screen_name)

    is_type_of(fn
      %User{} -> true
      _other -> false
    end)

    field(:id, non_null(:id))
    field(:screen_name, non_null(:string))
    field(:favorites_count, non_null(:integer), resolve: rsv(:favourites_count))

    field :favorites, non_null(list_of(non_null(:tweet))) do
      resolve(dispatched("favorites"))
      meta(ordered_by: :id, order: :desc, unique: true)
    end

    field :tweets, non_null(list_of(non_null(:tweet))) do
      resolve(dispatched("tweets"))
      meta(ordered_by: :id, order: :desc, unique: true)
    end

    field :retweets, non_null(list_of(non_null(:tweet))) do
      resolve(dispatched("retweets"))
      meta(ordered_by: :id, order: :desc, unique: true)
    end
  end

  object :tweet do
    interface(:node)

    is_type_of(fn
      %Tweet{} -> true
      _other -> false
    end)

    field(:id, non_null(:id))
    field(:text, non_null(:string), resolve: rsv(:full_text))
    field(:published_at, non_null(:string), resolve: rsv(:created_at))
    field(:likes_count, non_null(:integer), resolve: rsv(:favorite_count))
    field(:retweets_count, non_null(:integer), resolve: rsv(:retweet_count))
    field(:user, non_null(:twitter_user))
    field(:likes, non_null(list_of(non_null(:twitter_like))), resolve: dispatched("likes"))
    field(:mentions, non_null(list_of(non_null(:twitter_user))), resolve: dispatched("mentions"))

    field :retweets, non_null(list_of(non_null(:tweet))) do
      resolve(dispatched("retweets"))
      meta(ordered_by: :id, order: :desc, unique: true)
    end

    field :replies, non_null(list_of(non_null(:tweet))) do
      resolve(dispatched("replies"))
      meta(ordered_by: :id, order: :desc, unique: true)
    end

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

      resolve(fn _, %{id: "TwitterUser:" <> id}, _ ->
        obj = @twitter_client.user(id)
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
      fun = fn prev_min_id ->
        dispatched(obj, field, prev_min_id)
      end

      {:middleware, Weaver.Absinthe.Middleware.Dispatch, fun}
    end
  end

  defp rsv(field) when is_atom(field) do
    fn obj, _, _ -> Map.fetch(obj, field) end
  end

  def dispatched(obj = %User{}, "favorites", prev_min_id) do
    case prev_min_id do
      nil ->
        @twitter_client.favorites(id: obj.id, tweet_mode: :extended, count: @api_count)

      min_id ->
        @twitter_client.favorites(
          id: obj.id,
          tweet_mode: :extended,
          count: @api_count,
          max_id: min_id - 1
        )
    end
    |> case do
      [] -> {:done, []}
      tweets -> {:continue, Enum.take(tweets, @api_take), cursor(tweets)}
    end
  end

  def dispatched(obj = %User{}, "tweets", prev_min_id) do
    case prev_min_id do
      nil ->
        @twitter_client.user_timeline(
          screen_name: obj.screen_name,
          include_rts: false,
          tweet_mode: :extended,
          count: @api_count
        )

      min_id ->
        @twitter_client.user_timeline(
          screen_name: obj.screen_name,
          include_rts: false,
          tweet_mode: :extended,
          count: @api_count,
          max_id: min_id - 1
        )
    end
    |> case do
      [] -> {:done, []}
      tweets -> {:continue, Enum.take(tweets, @api_take), cursor(tweets)}
    end
  end

  def dispatched(obj = %User{}, "retweets", prev_min_id) do
    case prev_min_id do
      nil ->
        @twitter_client.user_timeline(
          screen_name: obj.screen_name,
          tweet_mode: :extended,
          count: @api_count
        )

      min_id ->
        @twitter_client.user_timeline(
          screen_name: obj.screen_name,
          tweet_mode: :extended,
          count: @api_count,
          max_id: min_id - 1
        )
    end
    |> Enum.filter(& &1.retweeted_status)
    |> case do
      [] -> {:done, []}
      tweets -> {:continue, Enum.take(tweets, @api_take), cursor(tweets)}
    end
  end

  def dispatched(%Tweet{}, "likes", _prev_min_id) do
    {:done, []}
  end

  def dispatched(%Tweet{}, "replies", _prev_min_id) do
    {:done, []}
  end

  def dispatched(obj = %Tweet{}, "retweets", prev_min_id) do
    case prev_min_id do
      nil ->
        @twitter_client.retweets(obj.id, count: @api_count, tweet_mode: :extended)

      min_id ->
        @twitter_client.retweets(obj.id,
          count: @api_count,
          tweet_mode: :extended,
          max_id: min_id - 1
        )
    end
    |> case do
      [] -> {:done, []}
      tweets -> {:continue, Enum.take(tweets, @api_take), cursor(tweets)}
    end
  end

  def dispatched(obj = %Tweet{}, "mentions", _prev_min_id) do
    users =
      case obj.entities.user_mentions do
        [] -> []
        mentions -> mentions |> Enum.map(& &1.id) |> @twitter_client.user_lookup()
      end

    {:done, users}
  end

  def cursor(tweets) do
    tweets
    |> List.last()
    |> Map.get(:id)
  end
end
