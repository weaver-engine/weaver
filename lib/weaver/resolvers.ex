defmodule Weaver.Resolvers do
  @moduledoc """
  Weaver-style GraphQL resolvers used in tests.

  Resolvers can return `:dispatch` to declare that the resolver should
  run in another, dispatched, step to resolve this predicate/edge. It will be
  returned as dispatched step and should be handled by the caller.
  """

  alias Weaver.{Marker, Ref}
  alias ExTwitter.Model.{Tweet, User}

  @twitter_client Application.get_env(:weaver, :twitter)[:client_module]
  @api_count Application.get_env(:weaver, :twitter)[:api_count]
  @api_take Application.get_env(:weaver, :twitter)[:api_take]

  def retrieve_by_id("TwitterUser:" <> id) do
    @twitter_client.user(id)
  end

  def id_for(obj = %User{}), do: "TwitterUser:#{obj.screen_name}"
  def id_for(obj = %Tweet{}), do: "Tweet:#{obj.id_str}"

  def end_marker(objs) when is_list(objs) do
    objs
    |> Enum.min_by(& &1.id)
    |> marker(:chunk_end)
  end

  def start_marker(objs) when is_list(objs) do
    objs
    |> Enum.max_by(& &1.id)
    |> marker(:chunk_start)
  end

  def marker_val(%{id: val}), do: val

  def marker(obj, type) do
    %Marker{type: type, ref: Ref.from(obj), val: marker_val(obj)}
  end

  def resolve_leaf(obj = %User{}, "screenName") do
    obj.screen_name
  end

  def resolve_leaf(obj = %User{}, "favoritesCount") do
    obj.favourites_count
  end

  def resolve_leaf(obj = %Tweet{}, "text") do
    obj.full_text
  end

  def resolve_leaf(obj = %Tweet{}, "publishedAt") do
    obj.created_at
  end

  def resolve_leaf(obj = %Tweet{}, "likesCount") do
    obj.favorite_count
  end

  def resolve_leaf(obj = %Tweet{}, "retweetsCount") do
    obj.retweet_count
  end

  def resolve_node(%User{}, "favorites"), do: :dispatch
  def resolve_node(%User{}, "tweets"), do: :dispatch
  def resolve_node(%User{}, "retweets"), do: :dispatch

  def resolve_node(obj = %Tweet{}, "user") do
    obj.user
  end

  def resolve_node(obj = %Tweet{}, "retweetOf") do
    obj.retweeted_status
  end

  def resolve_node(%Tweet{}, "likes"), do: :dispatch
  def resolve_node(%Tweet{}, "replies"), do: :dispatch
  def resolve_node(%Tweet{}, "retweets"), do: :dispatch
  def resolve_node(%Tweet{}, "mentions"), do: :dispatch

  def total_count(obj = %User{}, "favorites") do
    obj.favourites_count
  end

  def total_count(obj = %Tweet{}, "likesCount") do
    obj.favorite_count
  end

  def total_count(obj = %Tweet{}, "retweetsCount") do
    obj.retweet_count
  end

  def total_count(_obj, _relation), do: nil

  def dispatched(obj = %User{}, "favorites", prev_end_marker) do
    tweets =
      case prev_end_marker do
        nil ->
          @twitter_client.favorites(id: obj.id, tweet_mode: :extended, count: @api_count)

        %Marker{val: min_id} ->
          @twitter_client.favorites(
            id: obj.id,
            tweet_mode: :extended,
            count: @api_count,
            max_id: min_id - 1
          )
      end

    case tweets do
      [] ->
        {:done, []}

      tweets ->
        {:continue, Enum.take(tweets, @api_take), end_marker(tweets)}
    end
  end

  def dispatched(obj = %User{}, "tweets", prev_end_marker) do
    tweets =
      case prev_end_marker do
        nil ->
          @twitter_client.user_timeline(
            screen_name: obj.screen_name,
            include_rts: false,
            tweet_mode: :extended,
            count: @api_count
          )

        %Marker{val: min_id} ->
          @twitter_client.user_timeline(
            screen_name: obj.screen_name,
            include_rts: false,
            tweet_mode: :extended,
            count: @api_count,
            max_id: min_id - 1
          )
      end

    case tweets do
      [] ->
        {:done, []}

      tweets ->
        {:continue, Enum.take(tweets, @api_take), end_marker(tweets)}
    end
  end

  def dispatched(obj = %User{}, "retweets", prev_end_marker) do
    tweets =
      case prev_end_marker do
        nil ->
          @twitter_client.user_timeline(
            screen_name: obj.screen_name,
            tweet_mode: :extended,
            count: @api_count
          )

        %Marker{val: min_id} ->
          @twitter_client.user_timeline(
            screen_name: obj.screen_name,
            tweet_mode: :extended,
            count: @api_count,
            max_id: min_id - 1
          )
      end

    case tweets do
      [] ->
        {:done, []}

      tweets ->
        tweets =
          tweets
          |> Enum.filter(& &1.retweeted_status)
          |> Enum.take(@api_take)

        {:continue, tweets, end_marker(tweets)}
    end
  end

  def dispatched(%Tweet{}, "likes", _prev_end_marker) do
    {:done, []}
  end

  def dispatched(%Tweet{}, "replies", _prev_end_marker) do
    {:done, []}
  end

  def dispatched(obj = %Tweet{}, "retweets", prev_end_marker) do
    tweets =
      case prev_end_marker do
        nil ->
          @twitter_client.retweets(obj.id, count: @api_count, tweet_mode: :extended)

        %Marker{val: min_id} ->
          @twitter_client.retweets(obj.id,
            count: @api_count,
            tweet_mode: :extended,
            max_id: min_id - 1
          )
      end

    case tweets do
      [] ->
        {:done, []}

      tweets ->
        {:continue, Enum.take(tweets, @api_take), end_marker(tweets)}
    end
  end

  def dispatched(obj = %Tweet{}, "mentions", _prev_end_marker) do
    users =
      case obj.entities.user_mentions do
        [] -> []
        mentions -> mentions |> Enum.map(& &1.id) |> @twitter_client.user_lookup()
      end

    {:done, users}
  end
end
