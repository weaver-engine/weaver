defmodule Weaver.GraphTest do
  use Weaver.IntegrationCase, async: false

  doctest Weaver.Graph

  import Weaver.Graph, only: [store: 2, store!: 2, query: 1, markers: 2, markers: 3]

  setup :use_graph

  setup do
    user1 = %Ref{id: "TwitterUser:elixirdigest"}
    user2 = %Ref{id: "TwitterUser:elixirlang"}

    {:ok, user1: user1, user2: user2}
  end

  describe "data integrity" do
    setup %{user1: user1, user2: user2} do
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

      :ok
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

    test "markers", %{user1: user1} do
      assert {:ok, [marker1, marker2]} = markers(user1, "favorites", limit: 2)
      assert marker1 == Marker.chunk_start("Tweet:140", 140)
      assert marker2 == Marker.chunk_end("Tweet:134", 134)
    end

    test "delete markers", %{user1: user1} do
      assert %{} = store!([], [{:del, user1, "favorites", Marker.chunk_start("Tweet:140", 140)}])

      assert {:ok, [marker2]} = markers(user1, "favorites", limit: 1)
      assert marker2 == Marker.chunk_end("Tweet:134", 134)
    end

    test "delete and add same markers", %{user1: user1} do
      assert %{} =
               store!([], [
                 {:add, user1, "favorites", Marker.chunk_end("Tweet:140", 140)},
                 {:del, user1, "favorites", Marker.chunk_start("Tweet:140", 140)}
               ])

      assert {:ok, [marker2]} = markers(user1, "favorites", limit: 1)
      assert marker2 == Marker.chunk_end("Tweet:140", 140)
    end

    test "markers less_than", %{user1: user1} do
      assert {:ok, markers} = markers(user1, "favorites", less_than: 134)

      assert markers == [
               Marker.chunk_start("Tweet:34", 34),
               Marker.chunk_end("Tweet:34", 34),
               Marker.chunk_start("Tweet:4", 4),
               Marker.chunk_end("Tweet:3", 3),
               Marker.chunk_start("Tweet:1", 1)
             ]
    end

    test "markers less_than + limit", %{user1: user1} do
      assert {:ok, markers} = markers(user1, "favorites", less_than: 134, limit: 1)

      assert markers == [Marker.chunk_start("Tweet:34", 34)]
    end
  end

  describe "no markers" do
    test "returns empty list", %{user1: user1} do
      assert {:ok, []} = markers(user1, "favorites")
    end
  end

  describe "validation" do
    test "invalid data", %{user1: user1} do
      assert {:error, _} = store([{user1, "likes", :nothing}], [])
    end

    test "invalid meta", %{user1: user1} do
      assert {:error, _} = store([], [{:add, user1, "likes", "bananas"}])
    end
  end
end
