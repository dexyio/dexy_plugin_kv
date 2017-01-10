defmodule DexyPluginKV.Adapters.Riak do
  
  use DexyLib, as: Lib
  require Logger

  @app :dexy_plugin_kv
  @behaviour DexyPluginKV.Adapter

  deferror Error.UserdataBucketNotConfigured
  deferror Error.UserdataBucketTypeNotConfigured
  deferror Error.UserdataContentTypeNotConfigured
  deferror Error.UserdataIndexNotConfigured

  @default_content_type "application/x-erlang-binary"

  @userdata_bucket_type Application.get_env(@app, __MODULE__)[:userdata_bucket_type]
    || raise Error.UserdataBucketTypeNotConfigured

  @userdata_content_type Application.get_env(@app, __MODULE__)[:userdata_content_type]
    || raise Error.UserdataContentTypeNotConfigured

  @userdata_index Application.get_env(@app, __MODULE__)[:userdata_index]
    || raise Error.UserdataIndexNotConfigured

  @key_delimiter Application.get_env(@app, __MODULE__)[:key_delimiter]
    || (Logger.warn "key_delimiter not configured, default: \"/\""; "/")


  defmodule Userdata do
    defstruct bucket: nil,
              key: nil,
              value: nil,
              created: nil
  end

  def start_link(host \\ '127.0.0.1', port \\ 8087) do
    :riakc_pb_socket.start_link(host, port)
  end
  
  @spec delete(bitstring, bitstring, bitstring) :: {:ok, term} | {:error, term}

  def get user, bucket, key do
    case get_object riak_bucket(user), riak_key(bucket, key) do
      {:ok, obj} -> {:ok, value obj}
      err = {:error, _reason} -> err
    end
  end

  @spec put(bitstring, bitstring, bitstring, any, Keyword.t) :: :ok | {:error, term}

  def put user, bucket, key, val, _opts \\ [] do
    {key, val} = riak_keyval bucket, key, val
    riak_bucket(user)
      |> new_object(key, val, @userdata_content_type)
      |> put_object
  end

  @spec delete(bitstring, bitstring, bitstring) :: :ok | {:error, term}

  def delete(user, bucket, key) do
    key = riak_key bucket, key
    pool &:riakc_pb_socket.delete(&1, riak_bucket(user), key)
  end

  @spec get_all(bitstring, bitstring) :: {:ok, term} | {:error, term}

  def get_all user, bucket do
    search user: user, bucket: bucket
  end

  def delete_all _user, _bucket do
  end

  def buckets _user do
    []
  end

  def keys _user, _bucket do
    []
  end

  @spec search(Keyword.t, Keyword.t) :: {:ok, term} | {:error, term}

  def search query_opts, search_opts \\ [] do
    user = query_opts[:user]
    bucket = query_opts[:bucket]
    key = query_opts[:key]
    (user && "_yz_rb:#{user}" || "")
      <> (bucket && " AND bucket:#{bucket}" || "")
      <> (key && " AND key:#{key}" || "")
    |> do_search(search_opts)
  end 

  defp do_search query, opts do
    IO.puts query
    options = opts[:options] || []
    timeout = opts[:timeout] || default_timeout(:search)
    case pool &:riakc_pb_socket.search(&1, @userdata_index, query, options, timeout) do
      {:ok, {:search_results, list, _, _total}} ->
        res = list |> Enum.map(fn {_idx_name, items} ->
          [
            List.keyfind(items, "bucket", 0, {"bucket", nil}),
            List.keyfind(items, "key", 0, {"key", nil}),
            List.keyfind(items, "value", 0, {"value", nil}),
            List.keyfind(items, "created", 0, {"created", nil})
          ] |> Enum.into(%{})
        end)
        {:ok, res}
      {:error, _reason} = error ->
        error
    end
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
      bin when is_binary(bin) -> Lib.binary_to_term(bin)[:value]
      error ->error
    end
  end

  defp default_timeout(:search) do
    :riakc_pb_socket.default_timeout(:search_timeout)
  end

  defp default_timeout(:put) do
    :riakc_pb_socket.default_timeout(:put_timeout)
  end

  defp riak_bucket user do
    {@userdata_bucket_type, user}
  end

  defp riak_key bucket, key do
    bucket <> @key_delimiter <> key
  end

  defp riak_keyval bucket, key, val do
    riak_val = %Userdata{
      bucket: bucket,
      key: key,
      value: val,
      created: Lib.now(:usecs)
    } |> Map.from_struct |> Map.to_list |> Lib.to_binary
    {riak_key(bucket, key), riak_val}
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
