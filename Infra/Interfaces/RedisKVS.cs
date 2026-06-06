using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using StackExchange.Redis;

namespace Infra.Interfaces;

public class RedisKVS : IKeyValueStore
{
	private readonly Dictionary<string, IConnectionMultiplexer> _connections = new();
	private readonly IDatabase _db;

	// Use logger if necessary
	private readonly ILogger<RedisKVS> logger;

	public RedisKVS(ILogger<RedisKVS> logger)
	{
		this._connections["Primary"] = ConnectionMultiplexer.Connect(Constants.RedisPrimary);
		this._db = this._connections["Primary"].GetDatabase();
		// this._connections["Secondary"] = ConnectionMultiplexer.Connect(Constants.RedisSecondary);
		this.logger = logger;
	}

	public string GetString(string key)
	{
		RedisValue value = this._db.StringGet(key);
		return value.HasValue ? value : "";
	}

	public bool PutString(string key, string value)
	{
		return this._db.StringSet(key, value);
	}

    public T Get<T>(string key)
	{
		RedisValue value = this._db.StringGet(key);
		return value.HasValue ? JsonConvert.DeserializeObject<T>(value) : default;
	}

	public bool Put<T>(string key, T value)
	{
		var serialized = JsonConvert.SerializeObject(value);
		return this._db.StringSet(key, serialized);
	}

	public void Reset()
	{
		this._db.Execute("flushdb");
	}

}

