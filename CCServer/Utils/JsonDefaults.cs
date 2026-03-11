using System.Text.Json;

namespace CCServer.Utils
{
    public class JsonDefaults
    {
        // ASP.NET Core 기본(Web)과 동일: camelCase 등
        public static readonly JsonSerializerOptions Web = new(JsonSerializerDefaults.Web);
    }
}
