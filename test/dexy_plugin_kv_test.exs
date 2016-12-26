defmodule DexyPluginKVTest do
  use ExUnit.Case
  doctest DexyPluginKV

  alias DexyPluginKV, as: KV

  test "the truth" do
    Application.start :pooler
    opts = %{}
    KV.put %{args: ["foo"], opts: opts}
    assert {_, "foo"} = KV.get %{args: [], opts: opts}
  end
end
