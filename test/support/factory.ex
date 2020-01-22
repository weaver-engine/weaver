defmodule Test.Support.Factory do
  @moduledoc "Generate test data."

  def build(type, fields_or_count \\ [], fun \\ nil)

  def build(type, count, nil) when is_integer(count) do
    Range.new(0, count - 1)
    |> Enum.map(fn _i -> build(type) end)
  end

  def build(type, count, fun) when is_integer(count) do
    Range.new(0, count - 1)
    |> Enum.map(fn i -> build(type, fun.(i)) end)
  end

  def build(type, fields, _fun) when is_list(fields) do
    struct!(type, fields_for(type, fields))
  end

  def fields_for(type, fields \\ [])

  def fields_for(ExTwitter.Model.User, fields) do
    {id, fields} = Keyword.pop(fields, :id, twitter_id())

    %{
      id: id,
      id_str: Integer.to_string(id),
      screen_name: Faker.Internet.user_name(),
      favourites_count: 0,
      followers_count: 0,
      friends_count: 0,
      listed_count: 0,
      statuses_count: 0
    }
    |> merge(fields)
  end

  def fields_for(ExTwitter.Model.Tweet, fields) do
    {id, fields} = Keyword.pop(fields, :id, twitter_id())

    %{
      id: id,
      id_str: Integer.to_string(id),
      full_text: Faker.Lorem.sentence(),
      user: build(ExTwitter.Model.User),
      created_at: "2019-12-12T13:37:00",
      favorite_count: 0,
      retweet_count: 0
    }
    |> merge(fields)
  end

  defp merge(map, []) when is_map(map) do
    map
  end

  defp merge(map, [{k, v} | fields]) when is_map(map) do
    Map.put(map, k, v)
    |> merge(fields)
  end

  def twitter_id() do
    :rand.uniform(1_000_000_000)
  end
end
