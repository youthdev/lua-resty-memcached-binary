# lua-resty-memcached-binary
Nginx lua binary memcached protocol

This library leverages some implementation of Memcached and adjust it to support only Memcached cluster. It's well tested with Apache Ignite cluster.
Thank's for the ideas: [Lua-memcached](https://github.com/kolchanov/Lua-memcached)

Table of Contents
=============

* [Status](#status)
* [Synopsis](#synopsis)
	* [Module](#module)
	* [Config](#config)
* [Cluster API](#cluster_api)
* [Bucket API](#bucket_api)
* [Session API](#session_api)
    * [noop](#noop)
    * [close](#close)
    * [setkeepalive](#setkeepalive)
    * [set](#set)
    * [setQ](#setq)
    * [add](#add)
    * [replace](#replace)
    * [replaceQ](#replaceq)
    * [get](#get)
    * [getK](#getk)
    * [touch](#touch)
    * [gat](#gat)
    * [delete](#delete)
    * [deleteQ](#deleteq)
    * [increment](#increment)
    * [incrementQ](#incrementq)
    * [decrement](#decrement)
    * [decrementQ](#decrementq)
    * [append](#append)
    * [appendQ](#appendq)
    * [prepend](#prepend)
    * [prependQ](#prependq)
    * [stat](#stat)
    * [version](#version)
    * [sasl_list](#sasl_list)
* [Async API](#async_api)
    * [send](#send)
    * [receive](#receive)
    * [batch](#receive)
* [N1QL](#n1ql)

Status
=====

Production ready.

Synopsis
=======

Module "example"
-------
```
local _M = {
  _VERSION = "1.0"
}

local bmemcached = require "resty.bmemcached"

-- cluster
local cluster = bmemcached.cluster({
  host = "10.0.10.2"
})

-- select bucket
local bucket1 = cluster:bucket()

function _M.test_b1(key, value)
  local ses = bucket1:session()
  local r = ses:set(key, value)
  r = ses:get(key)
  ses:setkeepalive()
  return r
end

return _M

```

Config
------
```
server {
  listen 4444;
  location /test_b1 {
    content_by_lua_block {
      local ex = require "example"
      local cjson = require "cjson"
      ngx.say(cjson.encode(ex.test_b1(ngx.var.arg_key, ngx.var.arg_value)))
    }
  }
  location /test_b2 {
    content_by_lua_block {
      local ex = require "example"
      local cjson = require "cjson"
      ngx.say(cjson.encode(ex.test_b2(ngx.var.arg_key, ngx.var.arg_value)))
    }
  }
}
```

<a name="cluster_api"></a>
Cluster API
========
cluster
------
**syntax:** `cluster(opts)`

**context:** rewrite_by_lua, access_by_lua, content_by_lua, timer

Create the cluster object.

`opts` - the table with parameters.

* host - Memcached REST API host.
* port - Memcached REST API port (default 8091).
* user, password - username and password for REST api.
* timeout - http timeout.

```
local memcached = require "resty.bmemcached"

local cluster = memcached.cluster {
  host = "10.0.10.2"
}
```

**return:** cluster object or throws the error.

bucket
------
**syntax:** `cluster:bucket(opts)`

**context:** rewrite_by_lua, access_by_lua, content_by_lua, timer

Create the bucket object.

```
local bucket = cluster:bucket()
```

`opts` - the table with parameters.

* name - Memcached BUCKET name (default: `default`).
* timeout - socket timeout.
* pool_size - bucket keepalive pool size.
* pool_idle - bucket keepalive pool idle in sec.

**return:** bucket object or throws the error.

<a name="bucket_api"></a>
Bucket API
========
session
------
**syntax:** `bucket:session()`

**context:** rewrite_by_lua, access_by_lua, content_by_lua, timer

Create the session object.

```
local bucket = bucket:session()
```

**return:** session object or throws the error.

<a name="session_api"></a>
Session API
========
noop
------
**syntax:** `session:noop()`

**context:** rewrite_by_lua, access_by_lua, content_by_lua, timer

No operation.

Noop operation over all connections in the current bucket session is used.

**return:** array with memcached responses `[{"header":{"opaque":0,"CAS":[0,0,0,0,0,0,0,0],"status_code":0,"status":"No error","type":0}}]`.

close
------
**syntax:** `session:close()`

**context:** rewrite_by_lua, access_by_lua, content_by_lua, timer

Close all connections to all vbuckets in the current bucket session.

**return:** none

setkeepalive
------------
**syntax:** `session:setkeepalive()`

**context:** rewrite_by_lua, access_by_lua, content_by_lua, timer

Return all connections in the current session to keepalive bucket pool.

**return:** none

set
---
**syntax:** `session:set(key, value, expire, cas)`

**context:** rewrite_by_lua, access_by_lua, content_by_lua, timer

Sets the `value` for the `key`.

Optional parameter `expire` sets the TTL for key.  
Optional parameter `cas` must be a CAS value from the `get()` method.  

**return:** `{"header":{"opaque":0,"CAS":[0,164,136,177,61,99,242,140],"status_code":0,"status":"No error","type":0}}` on success (or any valid memcached status) or throws the error.  
Status MUST be retrieved from the header.

setQ
----
**syntax:** `session:setQ(key, value, expire, cas)`

**context:** rewrite_by_lua, access_by_lua, content_by_lua, timer

Sets the `value` for the `key`.

Optional parameter `expire` sets the TTL for key.  
Optional parameter `cas` must be a CAS value from the `get()` method.  

Memcached not sent the response on setQ command.

**Example:**
```
  local peers = {}

  for i=1,n
  do
    local w = session:setQ(key, "xxxxxxxxxxxxxxx")
    local sock, pool = unpack(w.peer)
    peers[pool] = w.peer
  end

  -- wait responses (only errors)

  for peer in pairs(peers)
  do
    cb:receive(peer)
  end
```

**return:** `{"header":{"opaque":0,"CAS":[0,164,136,177,61,99,242,140],"status_code":0,"status":"No error","type":0}}` on success or throws the error.

add
---
**syntax:** `session:add(key, value, expire)`

**context:** rewrite_by_lua, access_by_lua, content_by_lua, timer

Add the `key` with `value`.

Optional parameter `expire` sets the TTL for key.

**return:** `{"header":{"opaque":0,"CAS":[0,164,136,177,61,99,242,140],"status_code":0,"status":"No error","type":0}}` on success (or any valid memcached status) or throws the error.  
Status MUST be retrieved from the header.

addQ
----
**syntax:** `session:addQ(key, value, expire)`

**context:** rewrite_by_lua, access_by_lua, content_by_lua, timer

Add the `key` with `value`.

Optional parameter `expire` sets the TTL for key.

Memcached not sent the response on addQ command.

**return:** `{"peer":{"sock":userdata,"pool":"addr/bucket"},"header":{"opaque":2142342}}` on success or throws the error.

replace
-------
**syntax:** `session:replace(key, value, expire, cas)`

**context:** rewrite_by_lua, access_by_lua, content_by_lua, timer

Replace the `value` for the `key`.

Optional parameter `expire` sets the TTL for key.  
Optional parameter `cas` must be a CAS value from the `get()` method.  

**return:** `{"header":{"opaque":0,"CAS":[0,164,137,158,82,69,97,51],"status_code":0,"status":"No error","type":0}}` on success (or any valid memcached status) or throws the error.
If key is not exists `{"header":{"opaque":0,"CAS":[0,0,0,0,0,0,0,0],"status_code":1,"status":"Key not found","type":0},"value":"Not found"}`.  
Status MUST be retrieved from the header.

replaceQ
--------
**syntax:** `session:replaceQ(key, value, expire, cas)`

**context:** rewrite_by_lua, access_by_lua, content_by_lua, timer

Replace the `value` for the `key`.

Optional parameter `expire` sets the TTL for key.  
Optional parameter `cas` must be a CAS value from the `get()` method.  

Memcached not sent the response on replaceQ command.

**return:** `{"peer":{"sock":userdata,"pool":"addr/bucket"},"header":{"opaque":2142342}}` on success or throws the error.

get
---
**syntax:** `session:get(key)`

**context:** rewrite_by_lua, access_by_lua, content_by_lua, timer

Get value for the `key`.

**return:** `{"header":{"opaque":0,"CAS":[0,164,133,238,116,1,16,213],"status_code":0,"status":"No error","type":0},"value":"7"}` on success (or any valid memcached status) or throws the error.
If key is not exists `{"header":{"opaque":0,"CAS":[0,0,0,0,0,0,0,0],"status_code":1,"status":"Key not found","type":0},"value":"Not found"}`.  
Status MUST be retrieved from the header.

getK
----
**syntax:** `session:getK(key)`

**context:** rewrite_by_lua, access_by_lua, content_by_lua, timer

Get value for the `key`.

K-version for `get` returns the `key` as additional parameter.

touch
-----
**syntax:** `session:touch(key, expire)`

**context:** rewrite_by_lua, access_by_lua, content_by_lua, timer

Get value for the `key`.

Optional parameter `expire` sets the TTL for key.

**return:** `{"header":{"opaque":0,"CAS":[0,164,140,177,69,146,148,224],"status_code":0,"status":"No error","type":0}}` on success (or any valid memcached status) or throws the error.
If key is not exists `{"header":{"opaque":0,"CAS":[0,0,0,0,0,0,0,0],"status_code":1,"status":"Key not found","type":0},"value":"Not found"}`.  
Status MUST be retrieved from the header.

gat
---
**syntax:** `session:gat(key, expire)`

**context:** rewrite_by_lua, access_by_lua, content_by_lua, timer

get() + touch()

delete
------
**syntax:** `session:delete(delete, cas)`

**context:** rewrite_by_lua, access_by_lua, content_by_lua, timer

Delete the `key`.

Optional parameter `cas` must be a CAS value from the `get()` method.

**return:** `{"header":{"opaque":0,"CAS":[0,164,140,177,69,146,148,224],"status_code":0,"status":"No error","type":0}}` on success (or any valid memcached status) or throws the error.
If key is not exists `{"header":{"opaque":0,"CAS":[0,0,0,0,0,0,0,0],"status_code":1,"status":"Key not found","type":0},"value":"Not found"}`.  
Status MUST be retrieved from the header.

deleteQ
-------
**syntax:** `session:deleteQ(delete, cas)`

**context:** rewrite_by_lua, access_by_lua, content_by_lua, timer

Delete the `key`.

Optional parameter `cas` must be a CAS value from the `get()` method.

Memcached not sent the response on deleteQ command.

**return:** `{"peer":{"sock":userdata,"pool":"addr/bucket"},"header":{"opaque":2142342}}` on success or throws the error.

increment
---------
**syntax:** `session:increment(key, increment, initial, expire)`

**context:** rewrite_by_lua, access_by_lua, content_by_lua, timer

Increment value for the `key`.

Optional parameter `increment` sets the increment value.
Optional parameter `initial` sets the initial value.
Optional parameter `expire` sets the TTL for key.

**return:** `{"header":{"opaque":0,"CAS":[0,164,139,76,53,235,109,100],"status_code":0,"status":"No error","type":0},"value":213}` on success (or any valid memcached status) or throws the error.
Returns the next value.  
Status MUST be retrieved from the header.  

incrementQ
----------
**syntax:** `session:incrementQ(key, increment, initial, expire)`

**context:** rewrite_by_lua, access_by_lua, content_by_lua, timer

Increment value for the `key`.

Optional parameter `increment` sets the increment value.  
Optional parameter `initial` sets the initial value.  
Optional parameter `expire` sets the TTL for key.  

Memcached not sent the response on incrementQ command.

**return:** `{"peer":{"sock":userdata,"pool":"addr/bucket"},"header":{"opaque":2142342}}` on success or throws the error.
Status MUST be retrieved from the header.

decrement
---------
**syntax:** `session:decrement(key, increment, initial, expire)`

**context:** rewrite_by_lua, access_by_lua, content_by_lua, timer

Decrement value for the `key`.

Optional parameter `increment` sets the decrement value.  
Optional parameter `initial` sets the initial value.  
Optional parameter `expire` sets the TTL for key.  

**return:** `{"header":{"opaque":0,"CAS":[0,164,139,76,53,235,109,100],"status_code":0,"status":"No error","type":0},"value":213}` on success (or any valid memcached status) or throws the error.
Returns the next value.  
Status MUST be retrieved from the header.

decrementQ
----------
**syntax:** `session:decrementQ(key, increment, initial, expire)`

**context:** rewrite_by_lua, access_by_lua, content_by_lua, timer

Decrement value for the `key`.

Optional parameter `increment` sets the decrement value.  
Optional parameter `initial` sets the initial value.  
Optional parameter `expire` sets the TTL for key.  

Memcached not sent the response on decrementQ command.

**return:** `{"peer":{"sock":userdata,"pool":"addr/bucket"},"header":{"opaque":2142342}}` on success or throws the error.  
Returns the next value.  
Status MUST be retrieved from the header.

append
------
**syntax:** `session:append(key, value, cas)`

**context:** rewrite_by_lua, access_by_lua, content_by_lua, timer

Append the `key` with `value`.

Optional parameter `cas` must be a CAS value from the `get()` method.

**return:** `{"header":{"opaque":0,"CAS":[0,164,136,177,61,99,242,140],"status_code":0,"status":"No error","type":0}}` on success (or any valid memcached status) or throws the error.  
Status MUST be retrieved from the header.

appendQ
-------
**syntax:** `session:appendQ(key, value, cas)`

**context:** rewrite_by_lua, access_by_lua, content_by_lua, timer

Append the `key` with `value`.

Optional parameter `cas` must be a CAS value from the `get()` method.

Memcached not sent the response on appendQ command.

**return:** `{"peer":{"sock":userdata,"pool":"addr/bucket"},"header":{"opaque":2142342}}` on success or throws the error.

prepend
-------
**syntax:** `session:prepend(key, value, cas)`

**context:** rewrite_by_lua, access_by_lua, content_by_lua, timer

Prepend the `key` with `value`.

Optional parameter `cas` must be a CAS value from the `get()` method.

**return:** `{"header":{"opaque":0,"CAS":[0,164,136,177,61,99,242,140],"status_code":0,"status":"No error","type":0}}` on success (or any valid memcached status) or throws the error.  
Status MUST be retrieved from the header.

prependQ
--------
**syntax:** `session:prependQ(key, value, cas)`

**context:** rewrite_by_lua, access_by_lua, content_by_lua, timer

Prepend the `key` with `value`.

Optional parameter `cas` must be a CAS value from the `get()` method.

Memcached not sent the response on prependQ command.

**return:** `{"peer":{"sock":userdata,"pool":"addr/bucket"},"header":{"opaque":2142342}}` on success or throws the error.

stat
----
**syntax:** `session:stat(key)`

**context:** rewrite_by_lua, access_by_lua, content_by_lua, timer

Get memcached different parameters.

If the `key` parameter found, then only for `key` information will be returned.

**return:** `[{"header":{"opaque":0,"CAS":[0,0,0,0,0,0,0,0],"status_code":0,"status":"No error","type":0},"key":"ep_config_file"}]` on success (or any valid memcached status) or throws the error.

version
-------
**syntax:** `session:version()`

**context:** rewrite_by_lua, access_by_lua, content_by_lua, timer

Get memcached version.

**return:** `{"header":{"opaque":0,"CAS":[0,0,0,0,0,0,0,0],"status_code":0,"status":"No error","type":0},"value":"3.1.6"}` on success (or any valid memcached status) or throws the error.

sasl_list
---------
**syntax:** `session:sasl_list()`

**context:** rewrite_by_lua, access_by_lua, content_by_lua, timer

Get available sasl methods.

**return:** `{"header":{"opaque":0,"CAS":[0,0,0,0,0,0,0,0],"status_code":0,"status":"No error","type":0},"value":"CRAM-MD5 PLAIN"}` on success (or any valid memcached status) or throws the error.

<a name="async_api"></a>
Async API
=========
send
----
**syntax:** `session:send(op, opts)`

**context:** rewrite_by_lua, access_by_lua, content_by_lua, timer

Send request to memcached .

Parameters `op` must be a constant.

**Example 1:**
```
  local c = require "resty.memcached.consts"

  ...

  local w = session:send(c.op_code.Set, {
    key = 88, value = 1234567890, expire = 0
  })

  local l = session:receive(w.peer, {
    opaque = w.header.opaque
  })

  ...
```

**Example 2:**
```
  local c = require "resty.memcached.consts"

  ...

  local batch = {
    { 77, "1234567890", expire = 0 },
    { 88, "1234567890", expire = 0 },
    { 99, "1234567890", expire = 0 }
  }

  local peers = {}
  local opaques = {}

  for _,req in ipairs(batch)
  do
    local key, value = unpack(req)
    local w = session:send(c.op_code.SetQ, {
      key = key, value = value, expire = 0
    })
    local peer, opaque = w.peer, w.header.opaque
    peers[peer] = true
    opaques[opaque] = req
  end

  -- wait responses (only errors)

  for peer in pairs(peers)
  do
    local fails = session:receive(peer)
    for _,fail in ipairs(fails)
    do
      local header, key, value = fail.header, fail.key, fail.value
      local request = opaques[header.opaque]
      opaques[header.opaque] = {
        header = header, key = key, value = value, request = request
      }
    end
  end

  ...
```

**return:** `{"header":{"opaque":0}}` on success or throws the error.

receive
-------
**syntax:** `session:receive(peer, opts)`

**context:** rewrite_by_lua, access_by_lua, content_by_lua, timer

Receive responses from memcached.

Parameters `ops` is optional.

`opts` is a table:
* opaque - got from `send`.
* limit - limit number of received messages.

[Examples](#send)

**return:**
```
{
  {"header":{"opaque":23234234,"CAS":[0,234,134,218,216,1,160,113],"status_code":0,"status":"No error","type":0},"value":"1312"},
  {"header":{"opaque":56756756,"CAS":[0,222,132,248,116,1,112,212],"status_code":0,"status":"No error","type":0},"value":"1231"},
  {"header":{"opaque":24234234,"CAS":[0,121,142,278,16,12,196,211],"status_code":0,"status":"No error","type":0},"value":"4222"}
}
```

```
{
  {"header":{"opaque":23234234,"CAS":[0,234,134,218,216,1,160,113],"status_code":1,"status":"Key not found","type":0},"value":"Key not found"},
  {"header":{"opaque":56756756,"CAS":[0,222,132,248,116,1,112,212],"status_code":1,"status":"Key not found","type":0},"value":"Key not found"},
  {"header":{"opaque":24234234,"CAS":[0,121,142,278,16,12,196,211],"status_code":1,"status":"Key not found","type":0},"value":"Key not found"}
}
```

on success or throws the error.

batch
-----
**syntax:** `session:batch(b, opts)`

**context:** rewrite_by_lua, access_by_lua, content_by_lua, timer

Batch request to memcached.

Parameter `b` is a table with single requests.

Parameters `ops` is optional.

`opts` is a table:
* unacked_window - unacknowledged request/response window.
* thread_pool_size - number of concurent threads.

**Example:**
```
  local b = {
    { op = c.op_code.Set, opts = { key = 1, value = "1234567890" },
    { op = c.op_code.Set, opts = { key = 2, value = "1234567890" },
    { op = c.op_code.Set, opts = { key = 3, value = "1234567890" },
    { op = c.op_code.Set, opts = { key = 4, value = "1234567890" },
    { op = c.op_code.Set, opts = { key = 5, value = "1234567890" },
    { op = c.op_code.Set, opts = { key = 6, value = "1234567890" },
    { op = c.op_code.Set, opts = { key = 7, value = "1234567890" },
    { op = c.op_code.Set, opts = { key = 8, value = "1234567890" },
    { op = c.op_code.Set, opts = { key = 10, value = "1234567890" },
    { op = c.op_code.Set, opts = { key = 11, value = "1234567890" },
    { op = c.op_code.Set, opts = { key = 12, value = "1234567890" },
    { op = c.op_code.Set, opts = { key = 13, value = "1234567890" },
    { op = c.op_code.Set, opts = { key = 14, value = "1234567890" },
    { op = c.op_code.Set, opts = { key = 15, value = "1234567890" },
    { op = c.op_code.Set, opts = { key = 16, value = "1234567890" },
    { op = c.op_code.Set, opts = { key = 17, value = "1234567890" },
    { op = c.op_code.Set, opts = { key = 18, value = "1234567890" },
    { op = c.op_code.Set, opts = { key = 19, value = "1234567890" },
    { op = c.op_code.Set, opts = { key = 20, value = "1234567890" }
  }

  session:batch(b, {
    unacked_window = 2,
    thread_pool_size = 4
  })
```

Updates every item in  `b` table with `result` field.

**return:** none or throws the error.

N1QL
----
**syntax:** `session:query(statement, args, timeout_ms)`

**context:** rewrite_by_lua, access_by_lua, content_by_lua, timer

N1QL query.
