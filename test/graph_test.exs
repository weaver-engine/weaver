defmodule Weaver.GraphTest do
  use Weaver.IntegrationCase, async: false

  doctest Weaver.Graph

  describe "data integrity" do
    import Weaver.Graph, only: [store!: 2, query: 1, cursors: 3]

    setup :use_graph

    setup do
      user1 = %Ref{id: "TwitterUser:elixirdigest"}
      user2 = %Ref{id: "TwitterUser:elixirlang"}

      data = [
        {user1, "screenName", "elixirdigest"},
        {user1, "follows", user2},
        {user2, "screenName", "elixirlang"}
      ]

      meta = [
        {:add, user1, "favorites", Marker.chunk_start("Tweet:140", 140)},
        {:add, user1, "favorites", Marker.chunk_end("Tweet:134", 134)},
        {:add, user1, "favorites", Marker.chunk_start("Tweet:34", 34)},
        {:add, user1, "favorites", Marker.chunk_end("Tweet:34", 34)},
        {:add, user1, "favorites", Marker.chunk_start("Tweet:4", 4)},
        {:add, user1, "favorites", Marker.chunk_end("Tweet:3", 3)},
        {:add, user1, "favorites", Marker.chunk_start("Tweet:1", 1)}
      ]

      store!(data, meta)

      {:ok, user1: user1}
    end

    test "data" do
      query = ~s"""
      {
        user(func: eq(id, "TwitterUser:elixirdigest")) {
          screenName
        }
      }
      """

      assert {:ok, %{"user" => [%{"screenName" => "elixirdigest"}]}} = query(query)
    end

    test "cursors", %{user1: user1} do
      assert {:ok, [cursor1, cursor2]} = cursors(user1, "favorites", limit: 2)
      assert cursor1 == Marker.chunk_start("Tweet:140", 140)
      assert cursor2 == Marker.chunk_end("Tweet:134", 134)
    end

    test "delete cursors", %{user1: user1} do
      assert %{} = store!([], [{:del, user1, "favorites", Marker.chunk_start("Tweet:140", 140)}])

      assert {:ok, [cursor2]} = cursors(user1, "favorites", limit: 1)
      assert cursor2 == Marker.chunk_end("Tweet:134", 134)
    end

    test "delete and add same cursors", %{user1: user1} do
      assert %{} =
               store!([], [
                 {:add, user1, "favorites", Marker.chunk_end("Tweet:140", 140)},
                 {:del, user1, "favorites", Marker.chunk_start("Tweet:140", 140)}
               ])

      assert {:ok, [cursor2]} = cursors(user1, "favorites", limit: 1)
      assert cursor2 == Marker.chunk_end("Tweet:140", 140)
    end

    test "cursors less_than", %{user1: user1} do
      assert {:ok, cursors} = cursors(user1, "favorites", less_than: 134)

      assert cursors == [
               Marker.chunk_start("Tweet:34", 34),
               Marker.chunk_end("Tweet:34", 34),
               Marker.chunk_start("Tweet:4", 4),
               Marker.chunk_end("Tweet:3", 3),
               Marker.chunk_start("Tweet:1", 1)
             ]
    end
  end
end
