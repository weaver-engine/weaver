defmodule Weaver.IntegrationCase do
  @moduledoc """
  This module defines the test case to be used by
  tests that require setting up a store.

  Such tests rely on `Weaver.Store` and also
  import other functionality to make it easier
  to build common data structures and query the schema.

  Finally, if the test case interacts with the database,
  it cannot be async. For this reason, every test resets
  the store in use at the beginning of the test.
  """

  use ExUnit.CaseTemplate

  using do
    quote do
      alias Weaver.{Cursor, Ref}

      import Weaver.IntegrationCase
    end
  end

  def use_graph(_context) do
    Weaver.Graph.reset!()
  end
end
