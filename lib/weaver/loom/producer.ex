defmodule Weaver.Loom.Producer do
  @moduledoc """
  Represents a worker at the root `GenStage` level.
  Dispatched steps are passed to the `GenStage` level below after each call
  to `Weaver.Step.process/1`. A step is passed again to `Weaver.Step.process/1`
  as long as it returns a new `Weaver.Step` as `next`.

  Implements a `GenStage` `producer` that receives its steps via `add/1`.
  """
  use GenStage

  def start_link(_arg) do
    GenStage.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def add(events) do
    GenServer.cast(__MODULE__, {:add, events})
  end

  # CALLBACKS
  # =========
  @impl GenStage
  def init(_arg) do
    {:producer, []}
  end

  @impl GenStage
  def handle_cast({:add, events}, state) when is_list(events) do
    {:noreply, events, state}
  end

  def handle_cast({:add, event}, state) do
    {:noreply, [event], state}
  end

  @impl GenStage
  def handle_demand(_demand, state) do
    {:noreply, [], state}
  end
end
