--- @module Bmemcached

local _M = {
  _VERSION = '1.0.0'
}

local cjson = require "cjson"
local http = require "resty.http"
local bit = require "bit"

local c = require "resty.bmemcached.consts"
local encoder = require "resty.bmemcached.encoder"

local tcp = ngx.socket.tcp
local tconcat, tinsert, tremove = table.concat, table.insert, table.remove
local setmetatable = setmetatable
local assert, error = assert, error
local pairs, ipairs = pairs, ipairs
local json_decode, json_encode = cjson.decode, cjson.encode
local encode_base64 = ngx.encode_base64
local crc32 = ngx.crc32_short
local xpcall = xpcall
local traceback = debug.traceback
local thread_spawn, thread_wait = ngx.thread.spawn, ngx.thread.wait
local unpack = unpack
local rshift, band = bit.rshift, bit.band
local random = math.random
local ngx_log = ngx.log
local DEBUG, ERR = ngx.DEBUG, ngx.ERR
local HTTP_OK, HTTP_BAD_REQUEST, HTTP_NOT_FOUND = ngx.HTTP_OK, ngx.HTTP_BAD_REQUEST, ngx.HTTP_NOT_FOUND
local null = ngx.null

local defaults = {
  port = 11211,
  timeout = 30000,
  pool_idle = 10,
  pool_size = 10
}

-- consts

local op_code = c.op_code
local status = c.status
local vbucket_id = 0

-- encoder

local encode = encoder.encode
local handle_header = encoder.handle_header
local handle_body = encoder.handle_body
local put_i8 = encoder.put_i8
local put_i32 = encoder.put_i32
local put_i32 = encoder.put_i32
local get_i32 = encoder.get_i32

-- helpers

local function foreach(tab, f)
  for k,v in pairs(tab) do f(k,v) end
end

local function foreach_v(tab, f)
  for _,v in pairs(tab) do f(v) end
end

local function foreachi(tab, f)
  for _,v in ipairs(tab) do f(v) end
end

local zero_4 = encoder.pack_bytes(4, 0, 0, 0, 0)

-- class tables

--- @type BmemcachedCluster
local bmemcached_cluster = {}
--- @type BmemcachedBucket
--  @field #BmemcachedCluster cluster
local bmemcached_bucket = {}
--- @type BmemcachedSession
--  @field #BmemcachedBucket bucket
local bmemcached_session = {}

-- request

--- @type Request
local request_class = {}

--- @return #Request
local function create_request(bucket, peer)
  local sock, pool = unpack(peer)
  return setmetatable({
    bucket = bucket,
    sock = sock,
    pool = pool,
    unknown = {}
  }, { __index = request_class })
end

function request_class:get_unknown()
  return self.unknown
end

function request_class:try(fun)
  assert(xpcall(function()
    fun()
  end, function(err)
    self.sock:close()
    ngx_log(ERR, err, "\n", traceback())
    self.bucket.connections[self.pool] = nil
    return err
  end))
end

function request_class:receive(opaque, limit)
  local header, key, value
  self:try(function()
    local j, incr = 0, limit and 1 or 0
    while j < (limit or 1)
    do
      header = handle_header(assert(self.sock:receive(24)))
      key, value = handle_body(self.sock, header)

      if opaque then
        if opaque == header.opaque then
          return
        end
        self.unknown[header.opaque] = { header = header, key = key, value = value }
      end
      j = j + incr
    end
  end)
  return header, key, value
end

function request_class:sync(limit)
  self:receive(self:send(encode(op_code.Noop, {})), limit)
end

function request_class:send(packet)
  local bytes, opaque = unpack(packet)
  self:try(function()
    assert(self.sock:send(bytes))
  end)
  return opaque
end

local function request(bucket, peer, packet, fun)
  local req = create_request(bucket, peer)

  local opaque = req:send(packet)
  local header, key, value = req:receive(opaque)

  return {
    header = header,
    key = key,
    value = (fun and value) and fun(value) or value
  }, req:get_unknown()
end

local function requestQ(bucket, peer, packet)
  local req = create_request(bucket, peer)
  return { peer = peer, header = { opaque = req:send(packet) } }
end

local function request_until(bucket, peer, packet)
  local req = create_request(bucket, peer)
  local list = {}

  local opaque = req:send(packet)

  repeat
    local header, key, value = req:receive(opaque)
    if key and value then
      tinsert(list, {
        header = header,
        key = key,
        value = value
      })
    end
  until not key or not value

  return list
end

-- cluster class

--- @return #BmemcachedCluster
--  @param #table opts
function _M.cluster(opts)
  opts = opts or {}

  assert((opts.host or opts.socket), "host or socket argument is required")

  opts.port = opts.port or defaults.port
  opts.timeout = opts.timeout or defaults.timeout

  opts.buckets = {}

  return setmetatable(opts, {
    __index = bmemcached_cluster
  })
end

--- @return #BmemcachedBucket
--  @param #BmemcachedCluster self
function bmemcached_cluster:bucket(opts)
  opts = opts or {}

  opts.cluster = self

  opts.name = opts.name or "default"
  opts.timeout = opts.timeout or defaults.timeout
  opts.pool_idle = opts.pool_idle or defaults.pool_idle
  opts.pool_size = opts.pool_size or defaults.pool_size

  opts.map = self.buckets[opts.name]
  if not opts.map then
    opts.map = {}
    self.buckets[opts.name] = opts.map
  end

  return setmetatable(opts, {
    __index = bmemcached_bucket
  })
end

-- bucket class

--- @return #BmemcachedSession
--  @param #BmemcachedBucket self
function bmemcached_bucket:session()
  return setmetatable({
    bucket = self,
    connections = {}
  }, {
    __index = bmemcached_session
  })
end

-- session class

local function auth_sasl(peer, bucket)
  if not bucket.password then
    return
  end
  local auth_resp = request(bucket, peer, encode(op_code.SASL_Auth, {
    key = "PLAIN",
    value = put_i8(0) .. bucket.name .. put_i8(0) ..  bucket.password
  }))
  if not auth_resp.header or auth_resp.header.status_code ~= status.NO_ERROR then
    peer[1]:close()
    local err = auth_resp.header.status or c.status_desc[status.AUTH_ERROR]
    error("Not authenticated: " .. err)
  end
end

local function connect(self)
  local bucket = self.bucket
  local host = bucket.host
  local port = bucket.port
  local pool = host .. "/" .. bucket.name
  local sock = self.connections[pool]
  if sock then
    return { sock, pool }
  end
  sock = assert(tcp())
  sock:settimeout(bucket.timeout)
  assert(sock:connect(host, port, {
    pool = pool
  }))
  if assert(sock:getreusedtimes()) == 0 then
    -- connection created
    -- sasl
    auth_sasl({ sock, pool }, bucket)
  end
  self.connections[pool] = sock
  return { sock, pool }
end

local function setkeepalive(self)
  local pool_idle, pool_size = self.bucket.pool_idle * 1000, self.bucket.pool_size
  foreach_v(self.connections, function(sock)
    sock:setkeepalive(pool_idle, pool_size)
  end)
  self.connections = {}
end

local function close(self)
  foreach_v(self.connections, function(sock)
    requestQ(self, sock, encode(op_code.QuitQ, {}))
    sock:close()
  end)
  self.connections = {}
end

function bmemcached_session:clone()
  return setmetatable({
    bucket = self.bucket,
    connections = {}
  }, {
    __index = bmemcached_session
  })
end

function bmemcached_session:setkeepalive()
  setkeepalive(self)
end

function bmemcached_session:close()
  close(self)
end

function bmemcached_session:noop()
  local resp = {}
  foreach_v(self.connections, function(sock)
    tinsert(resp, request(self.bucket, sock, encode(op_code.Noop, {})))
  end)
  return resp
end

function bmemcached_session:flush()
  return request(self.bucket, connect(self), encode(op_code.Flush, {}))    
end

function bmemcached_session:flushQ()
  error("Unsupported")
end

local op_extras = {
  [op_code.Set]      = zero_4,
  [op_code.SetQ]     = zero_4,
  [op_code.Add]      = zero_4,
  [op_code.AddQ]     = zero_4,
  [op_code.Replace]  = zero_4,
  [op_code.ReplaceQ] = zero_4
}

function bmemcached_session:set(key, value, expire, cas)
  assert(key and value, "key and value required")
  return request(self.bucket, connect(self), encode(op_code.Set, {
    key = key,
    value = value,
    expire = expire or 0,
    extras = op_extras[op_code.Set],
    cas = cas,
    vbucket_id = vbucket_id
  }))
end

function bmemcached_session:setQ(key, value, expire, cas)
  assert(key and value, "key and value required")
  return requestQ(self.bucket, connect(self), encode(op_code.SetQ, {
    key = key,
    value = value,
    expire = expire or 0,
    extras = op_extras[op_code.SetQ],
    cas = cas,
    vbucket_id = vbucket_id
  }))
end

function bmemcached_session:add(key, value, expire)
  assert(key and value, "key and value required")
  return request(self.bucket, connect(self), encode(op_code.Add, {
    key = key,
    value = value,
    expire = expire or 0,
    extras = op_extras[op_code.Add],
    vbucket_id = vbucket_id
  }))
end

function bmemcached_session:addQ(key, value, expire)
  assert(key and value, "key and value required")
  return requestQ(self.bucket, connect(self), encode(op_code.AddQ, {
    key = key,
    value = value,
    expire = expire or 0,
    extras = op_extras[op_code.AddQ],
    vbucket_id = vbucket_id
  }))
end

function bmemcached_session:replace(key, value, expire, cas)
  assert(key and value, "key and value required")
  return request(self.bucket, connect(self), encode(op_code.Replace, {
    key = key,
    value = value,
    expire = expire or 0,
    extras = op_extras[op_code.Replace],
    cas = cas,
    vbucket_id = vbucket_id
  }))
end

function bmemcached_session:replaceQ(key, value, expire, cas)
  assert(key and value, "key and value required")
  return requestQ(self.bucket, connect(self), encode(op_code.ReplaceQ, {
    key = key,
    value = value,
    expire = expire or 0,
    extras = op_extras[op_code.ReplaceQ],
    cas = cas,
    vbucket_id = vbucket_id
  }))
end 

function bmemcached_session:get(key)
  assert(key, "key required")
  return request(self.bucket, connect(self), encode(op_code.Get, {
    key = key,
    vbucket_id = vbucket_id
  }))
end

function bmemcached_session:getQ(key)
  error("Unsupported")
end

function bmemcached_session:getK(key)
  assert(key, "key required")
  return request(self.bucket, connect(self), encode(op_code.GetK, {
    key = key,
    vbucket_id = vbucket_id
  }))
end

function bmemcached_session:getKQ(key)
  error("Unsupported")
end

function bmemcached_session:touch(key, expire)
  assert(key and expire, "key and expire required")
  return request(self.bucket, connect(self), encode(op_code.Touch, {
    key = key,
    expire = expire,
    vbucket_id = vbucket_id
  }))
end

function bmemcached_session:gat(key, expire)
  assert(key and expire, "key and expire required")
  return request(self.bucket, connect(self), encode(op_code.GAT, {
    key = key,
    expire = expire, 
    vbucket_id = vbucket_id
  }))
end

function bmemcached_session:gatQ(key, expire)
  error("Unsupported")
end

function bmemcached_session:gatKQ(key, expire)
  error("Unsupported")
end

function bmemcached_session:delete(key, cas)
  return request(self.bucket, connect(self), encode(op_code.Delete, {
    key = key,
    cas = cas,
    vbucket_id = vbucket_id
  }))
end

function bmemcached_session:deleteQ(key, cas)
  return requestQ(self.bucket, connect(self), encode(op_code.DeleteQ, {
    key = key,
    cas = cas,
    vbucket_id = vbucket_id
  }))
end

function bmemcached_session:increment(key, increment, initial, expire)
  local extras = zero_4                  ..
                 put_i32(increment or 1) ..
                 zero_4                  ..
                 put_i32(initial or 0)
  return request(self.bucket, connect(self), encode(op_code.Increment, {
    key = key, 
    expire = expire or 0,
    extras = extras,
    vbucket_id = vbucket_id
  }), function(value)
    return get_i32 {
      data = value,
      pos = 5
    }
  end)
end 

function bmemcached_session:incrementQ(key, increment, initial, expire)
  local extras = zero_4                  ..
                 put_i32(increment or 1) ..
                 zero_4                  ..
                 put_i32(initial or 0)
  return requestQ(self.bucket, connect(self), encode(op_code.IncrementQ, {
    key = key, 
    expire = expire or 0,
    extras = extras,
    vbucket_id = vbucket_id
  }))
end 

function bmemcached_session:decrement(key, decrement, initial, expire)
  local extras = zero_4                  ..
                 put_i32(decrement or 1) ..
                 zero_4                  ..
                 put_i32(initial or 0)
  return request(self.bucket, connect(self), encode(op_code.Decrement, {
    key = key, 
    expire = expire or 0,
    extras = extras,
    vbucket_id = vbucket_id
  }), function(value)
    return get_i32 {
      data = value,
      pos = 5
    }
  end)
end

function bmemcached_session:decrementQ(key, decrement, initial, expire)
  local extras = zero_4                  ..
                 put_i32(decrement or 1) ..
                 zero_4                  ..
                 put_i32(initial or 0)
  return requestQ(self.bucket, connect(self), encode(op_code.DecrementQ, {
    key = key, 
    expire = expire or 0,
    extras = extras,
    vbucket_id = vbucket_id
  }))
end

function bmemcached_session:append(key, value, cas)
  assert(key and value, "key and value required")
  return request(self.bucket, connect(self), encode(op_code.Append, {
    key = key,
    value = value,
    cas = cas,
    vbucket_id = vbucket_id
  }))
end

function bmemcached_session:appendQ(key, value, cas)
  assert(key and value, "key and value required")
  return requestQ(self.bucket, connect(self), encode(op_code.AppendQ, {
    key = key,
    value = value,
    cas = cas,
    vbucket_id = vbucket_id
  }))
end

function bmemcached_session:prepend(key, value, cas)
  assert(key and value, "key and value required")
  return request(self.bucket, connect(self), encode(op_code.Prepend, {
    key = key,
    value = value,
    cas = cas,
    vbucket_id = vbucket_id
  }))
end

function bmemcached_session:prependQ(key, value, cas)
  if not key or not value then
    return nil, "key and value required"
  end
  return requestQ(self.bucket, connect(self), encode(op_code.PrependQ, {
    key = key,
    value = value,
    cas = cas,
    vbucket_id = vbucket_id
  }))
end

function bmemcached_session:stat(key)
  return request_until(self.bucket, connect(self), encode(op_code.Stat, {
    key = key,
    opaque = 0
  }))
end

function bmemcached_session:version()
  return request(self.bucket, connect(self), encode(op_code.Version, {}))
end

function bmemcached_session:verbosity(level)
  if not level then
    return nil, "level required"
  end
  return request(self.bucket, connect(self), encode(op_code.Verbosity, {
    extras = put_i32(level)
  }))
end

function bmemcached_session:helo()
  error("Unsupported")
end

function bmemcached_session:sasl_list()
  return request(self.bucket, connect(self), encode(op_code.SASL_List, {}))
end

function bmemcached_session:set_vbucket()
  error("Unsupported")
end

function bmemcached_session:get_vbucket(key)
  error("Unsupported")
end

function bmemcached_session:del_vbucket()
  error("Unsupported")
end

function bmemcached_session:list_buckets()
  return request(self.bucket, connect(self), encode(op_code.List_buckets, {}))
end

function bmemcached_session:send(op, opts)
  assert(op and opts and opts.key, "op_code, opts and opts.key required")
  opts.extras = op_extras[op]
  local result = { requestQ(self.bucket, connect(self), encode(op, opts)) }
  opts.vbucket_id, opts.extras = nil, nil
  return unpack(result)
end

function bmemcached_session:receive(peer, opts)
  assert(peer, "peer required")

  opts = opts or {}

  local opaque, limit = opts.opaque, opts.limit
  local req = create_request(self.bucket, peer)

  if not opaque and not limit then
    -- wait all
    req:sync()
    -- return all responses
    return req:get_unknown()
  end

  if opaque then
    -- return response only for [opaque], other previous are ignored 
    return req:receive(opaque)
  end

  -- return [limit] responses

  req:sync(limit)

  return req:get_unknown()
end

function bmemcached_session:batch(b, opts)
  local threads = {}
  local j = 0

  local unacked_window, thread_pool_size = opts.unacked_window or 10, opts.thread_pool_size or 1

  local function get()
    j = j + 1
    return b[j]
  end

  local function thread()
    local window = 0
    local queue = {}
    local session = self:clone()
    repeat
      local req = get()
      if req then
        window = window + 1
        -- send
        req.w = session:send(req.op, req.opts)
        tinsert(queue, req)
        if window == unacked_window then
          for j=1, unacked_window / 5
          do
            req = tremove(queue, 1)
            req.result = session:receive(req.w.peer, {
              opaque = req.w.header.opaque
            })
            -- cleanup temporary
            req.w = nil
            window = window - 1
          end
        end
      end
    until not req
    -- wait all
    foreachi(queue, function(req)
      req.result = session:receive(req.w.peer, {
        opaque = req.w.header.opaque
      })
      req.w = nil
    end)
    session:setkeepalive()
    return true
  end

  local ok, err = xpcall(function()
    for j=1,thread_pool_size
    do
      local thr, err = thread_spawn(thread)
      assert(thr, err)
      tinsert(threads, thr)
    end
  end, function(err)
    ngx_log(ERR, err, "\n", traceback())
    return err
  end)

  -- wait all
  foreachi(threads, function(thr)
    thread_wait(thr)
  end)

  assert(ok, err)
end

local function encode_args(args)
  local tab = {}
  foreach(args, function(k,v)
    if type(v) == "string" then
      v = "\"" ..  v .. "\""
    end
    tinsert(tab, k .. "=" .. v)
  end)
  return tconcat(tab, "&")
end

return _M
