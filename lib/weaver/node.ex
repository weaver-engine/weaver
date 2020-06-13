defprotocol Weaver.Node do
  @fallback_to_any true

  @doc """
  Returns an identifier for a given term.

  The identifier must uniquely identify a record within the record's GraphQL type.

  It may be a String or number.
  """
  @spec id_for(any()) :: binary() | number()
  def id_for(term)
end

defimpl Weaver.Node, for: Any do
  def id_for(%{id: id}), do: id
  def id_for(%{"id" => id}), do: id

  def id_for(term) do
    raise Protocol.UndefinedError, protocol: Weaver.Node, value: term
  end
end
