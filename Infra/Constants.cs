namespace Infra;

public class Constants
{
    public const int SiloPort = 11111;
    public const int GatewayPort = 30000;
    public const string ClusterId = "LocalTestCluster";
    public const string ServiceId = "SFaaS";
    public const string CheckoutNamespace = "checkout";
    public const string CustomerNamespace = "customer";
    public const string InventoryNamespace = "inventory";
    public const string KafkaService = "127.0.0.1:9092";
    public const string ZooKeeperService = "127.0.0.1:2181";
    public const string CheckoutTopicGroup = "checkout-group";
    public const string InventoryTopicGroup = "inventory-group";
    public const string OutcomeTopicGroup = "outcome-group";
    public const string RedisPrimary = "127.0.0.1:6379";
    public const string RedisSecondary = "127.0.0.1:6380";
}
