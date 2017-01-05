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
              data: nil,
              created: nil
  end

  def start_link(host \\ '127.0.0.1', port \\ 8087) do
    :riakc_pb_socket.start_link(host, port)
  end

  def get user, bucket, key do
    case get_object riak_bucket(user), riak_key(bucket, key) do
      {:ok, obj} -> {:ok, value obj}
      err = {:error, _reason} -> err
    end
  end

  def put user, bucket, key, val, _opts \\ [] do
    {key, val} = riak_keyval bucket, key, val
    riak_bucket(user)
      |> new_object(key, val, @userdata_content_type)
      |> put_object
  end

  def delete(user, bucket, key) do
    key = riak_key bucket, key
    pool &:riakc_pb_socket.delete(&1, riak_bucket(user), key)
  end

  def get_all user, bucket do
    #search "_yz_rb:#{user} AND _yz_rk:#{bucket}/*"
    case search "_yz_rb:#{user} AND bucket:#{bucket}" do
      {:ok, {:search_results, list, _, _total}} ->
        res = list |> Enum.map(fn {_idx_name, items} ->
          %{
            "key" => elem(Enum.at(items, 3), 1),
            "value" => elem(Enum.at(items, 6), 1),
            "created" => elem(Enum.at(items, 5), 1)
          }
        end)
        {:ok, res}
      {:error, reason} ->
        {:error, reason}
    end
  end

  def delete_all _user, _bucket do
  end

  def buckets _user do
    []
  end

  def keys _user, _bucket do
    []
  end

  def search query, opts \\ [] do
    options = opts[:options] || []
    timeout = opts[:timeout] || default_timeout(:search)
    pool &:riakc_pb_socket.search(&1, @userdata_index, query, options, timeout)
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
      bin when is_binary(bin) -> Lib.binary_to_term(bin)[:data]
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
      data: val,
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
