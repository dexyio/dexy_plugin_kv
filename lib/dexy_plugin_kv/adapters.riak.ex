defmodule DexyPluginKV.Adapters.Riak do
  
  use DexyLib, as: Lib

  @behaviour DexyPluginKV.Adapter
  @default_content_type "application/x-erlang-binary"

  def start_link(host \\ '127.0.0.1', port \\ 8087) do
    :riakc_pb_socket.start_link(host, port)
  end

  def get bucket, key do
    case get_object bucket, key do
      {:ok, obj} -> {:ok, value obj}
      err = {:error, _reason} -> err
    end
  end

  def put bucket, key, val, opts \\ [] do
    type = opts[:content_type] || @default_content_type
    new_object(bucket, key, Lib.to_binary(val), type)
    |> put_object
  end

  def delete(bucket, key) do
    pool &:riakc_pb_socket.delete(&1, bucket, key)
  end

  defp put_object(object) do
    pool &:riakc_pb_socket.put(&1, object)
  end

  defp new_object(bucket, key, val, content_type) do
    :riakc_obj.new(bucket, key, val, content_type)
  end

  defp get_object(bucket, key) do
    pool &:riakc_pb_socket.get(&1, bucket, key)
  end

  defp value(object) do
    case :riakc_obj.get_value(object) do
      bin when is_binary(bin) -> Lib.binary_to_term(bin)
      error -> error
    end
  end

  defp pool(fun) do
    pid = take_member
    res = fun.(pid)
    return_member pid
    res
  end

  defp take_member, do: :pooler.take_member(__MODULE__)
  defp return_member(pid), do: :pooler.return_member(__MODULE__, pid, :ok)

end
