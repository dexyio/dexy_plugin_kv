defmodule DexyPluginKVTest do

  use ExUnit.Case
  doctest DexyPluginKV

  alias DexyPluginKV, as: KV

  test "the truth" do
    Application.start :pooler
    req = %{user: "*"}
    opts = %{}

    KV.delete %{req: req, args: [], opts: opts}
    assert {_, nil} = KV.get%{req: req, args: [], opts: opts}

    KV.put %{req: req, args: ["foo"], opts: opts}
    assert {_, "foo"} = KV.get %{req: req, args: [], opts: opts}
  end

end
