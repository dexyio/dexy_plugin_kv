defmodule DexyPluginKV.Adapters.Riak do
  
  use DexyLib, as: Lib
  require Logger

  @app :dexy_plugin_kv
  @behaviour DexyPluginKV.Adapter

  deferror Error.UserdataBucketNotConfigured
  deferror Error.UserdataBucketTypeNotConfigured
  deferror Error.UserdataContentTypeNotConfigured
  deferror Error.UserdataIndexNotConfigured

  _default_content_type = "application/x-erlang-binary"

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
              created: nil,
              tags: nil,
              idx_int: nil,
              idx_long: nil,
              idx_float: nil,
              idx_double: nil,
              idx_string: nil


    @type int32 :: integer
    @type int64 :: integer
    @type float32 :: float
    @type float64 :: float

    @type t :: %__MODULE__{
      bucket: bitstring | nil,
      key: bitstring | nil,
      value: any,
      created: pos_integer | nil,
      tags: list(bitstring) | nil,
      idx_int: int32 | nil,
      idx_long: int64 | nil,
      idx_float: float32 | nil,
      idx_double: float64 | nil,
      idx_string: bitstring | nil,
    }
  end

  def start_link(host \\ '127.0.0.1', port \\ 8087) do
    :riakc_pb_socket.start_link(host, port)
  end
  
  @spec get(bitstring, bitstring, bitstring) :: {:ok, term} | {:error, term}

  def get user, bucket, key do
    case get_object riak_bucket(user), riak_key(bucket, key) do
      {:ok, obj} -> {:ok, (value obj)[:value]}
      {:error, _reason} = err -> err
    end
  end

  @spec put(bitstring, bitstring, bitstring, any, Keyword.t) :: :ok | {:error, term}

  def put user, bucket, key, val, opts \\ %{} do
    {key, val} = riak_keyval bucket, key, val, opts
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
    search user, %{"bucket" => bucket}
  end

  def delete_all _user, _bucket do
  end

  def buckets _user do
    []
  end

  def keys _user, _bucket do
    []
  end

  @spec search(bitstring, map, Keyword.t) :: {:ok, term} | {:error, term}

  def search(user, query, search_opts \\ []) 
    
  def search(user, query, search_opts) when is_bitstring(query) do
    case query do
      "" -> "_yz_rb:#{user}"
      str -> "_yz_rb:#{user} AND " <> str
    end
    |> do_search(search_opts)
  end

  def search(user, query_opts, search_opts) when is_map(query_opts) do
    ("_yz_rb:#{user}"
      <> ((bucket = query_opts["bucket"]) && " AND bucket:#{bucket}" || "")
      <> ((key = query_opts["key"]) && " AND key:#{key}" || "")
      <> ((tags = query_opts["tags"]) && " AND tags:#{tags}" || "")
      <> ((idx_int = query_opts["idx_int"]) && " AND idx_int:#{idx_int}" || "")
      <> ((idx_long = query_opts["idx_long"]) && " AND idx_long:#{idx_long}" || "")
      <> ((idx_float = query_opts["idx_float"]) && " AND idx_float:#{idx_float}" || "")
      <> ((idx_double = query_opts["idx_double"]) && " AND idx_double:#{idx_double}" || "")
      <> ((idx_string = query_opts["idx_string"]) && " AND idx_string:#{idx_string}" || "")
    )
    |> do_search(search_opts)
  end 

  @riak_bucket_field "_yz_rb"
  @riak_key_field "_yz_rk"

  defp do_search query, opts do
    Logger.info "query: #{query}, opts: #{inspect opts}"
    {timeout, opts} = Keyword.pop opts, :timeout
    timeout = timeout || default_timeout(:search)
    case pool &:riakc_pb_socket.search(&1, @userdata_index, query, opts, timeout) do
      {:ok, {:search_results, list, _, _total}} ->
        res = list |> Enum.map(fn {_idx_name, items} ->
          {_, bucket} = List.keyfind items, @riak_bucket_field, 0 
          {_, key} = List.keyfind items, @riak_key_field, 0
          case get_object riak_bucket(bucket), key do
            {:ok, obj} -> value(obj) |> to_search_result
            {:error, _} -> [
              List.keyfind(items, "bucket", 0, {"bucket", nil}),
              List.keyfind(items, "key", 0, {"key", nil}),
              {"value", nil}
            ] |> Enum.into(%{})
          end
        end)
        {:ok, res}
      {:error, _reason} = error ->
        error
    end
  end

  defp to_search_result(props) when is_list(props) do
    fields = [:bucket, :key, :created, :value]
    for {k, v} <- props, k in fields, into: %{} do
      {to_string(k), v}
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
      bin when is_binary(bin) -> Lib.binary_to_term(bin)
      error ->error
    end
  end

  defp default_timeout(:search) do
    :riakc_pb_socket.default_timeout(:search_timeout)
  end

  defp default_timeout(:put) do
    :riakc_pb_socket.default_timeout(:put_timeout)
  end

  defp riak_bucket bucket do
    {@userdata_bucket_type, bucket}
  end

  defp riak_key bucket, key do
    bucket <> @key_delimiter <> key
  end

  defp riak_keyval bucket, key, val, opts do
    riak_val = [
      bucket: bucket,
      key: key,
      value: val,
      created: Lib.now(:usecs),
      tags: opts["tags"],
      idx_int: opts["idx_int"],
      idx_long: opts["idx_long"],
      idx_float: opts["idx_float"],
      idx_double: opts["idx_double"],
      idx_string: opts["idx_string"],
    ] |> Lib.to_binary
    {riak_key(bucket, key), riak_val}
  end

  defp pool(fun) do
    pid = take_member()
    res = fun.(pid)
    return_member pid
    res
  end

  defp take_member, do: :pooler.take_member(__MODULE__)
  defp return_member(pid), do: :pooler.return_member(__MODULE__, pid, :ok)

end
