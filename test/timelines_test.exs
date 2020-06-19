defmodule Weaver.TimelinesTest do
  use Weaver.IntegrationCase, async: false

  @query """
  query {
    node(id: "TwitterUser:elixirdigest") {
      ... on TwitterUser {
        screenName
        tweets {
          __weaver_id : id
          __weaver_type : __typename
          text
        }
      }
    }
  }
  """

  setup do
    user = build(TwitterUser, screen_name: "elixirdigest")

    {:ok, user: user}
  end

  test "works", %{user: user} do
    user_ref = %Ref{id: "TwitterUser:#{user.screen_name}"}
    # TWEETS
    tweet1 = build(Tweet, id: 35)
    tweet2 = build(Tweet, id: 21)

    @query
    |> Weaver.prepare(Schema)
    |> weave_initial(Twitter, :user, fn "elixirdigest" -> user end)
    |> assert_data([
      {user_ref, "screenName", "elixirdigest"}
    ])
    |> assert_meta([])
    |> assert_dispatched_paths([
      [%{name: "tweets"}, %{name: "node"}, %{name: nil}]
    ])
    |> refute_next()
    |> weave_dispatched(Twitter, :user_timeline, fn _ -> [tweet1, tweet2] end)
    |> assert_data([
      {%Ref{id: "Tweet:#{tweet2.id}"}, "text", tweet2.full_text},
      {%Ref{id: "Tweet:#{tweet2.id}"}, "__weaver_type", "Tweet"},
      {%Ref{id: "Tweet:#{tweet2.id}"}, "__weaver_id", "#{tweet2.id}"},
      {user_ref, "tweets", %Ref{id: "Tweet:#{tweet2.id}"}},
      {%Ref{id: "Tweet:#{tweet1.id}"}, "text", tweet1.full_text},
      {%Ref{id: "Tweet:#{tweet1.id}"}, "__weaver_type", "Tweet"},
      {%Ref{id: "Tweet:#{tweet1.id}"}, "__weaver_id", "#{tweet1.id}"},
      {user_ref, "tweets", %Ref{id: "Tweet:#{tweet1.id}"}}
    ])
    |> assert_meta([
      {:add, user_ref, "tweets", Marker.chunk_start("Tweet:#{tweet1.id}", tweet1.id)},
      {:add, user_ref, "tweets", Marker.chunk_end("Tweet:#{tweet2.id}", tweet2.id, tweet2.id)}
    ])
    |> assert_dispatched_paths([])
    |> assert_next_path([%{name: "tweets"}, %{name: "node"}, %{name: nil}])
    |> assert_next_state(%{prev_chunk_end: %Marker{val: 21}})
  end
end
