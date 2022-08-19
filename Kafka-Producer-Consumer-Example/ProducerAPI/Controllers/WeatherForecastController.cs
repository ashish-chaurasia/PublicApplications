using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;
using ProducerAPI.Model;

namespace ProducerAPI.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class WeatherForecastController : ControllerBase
    {
        private static readonly string[] Summaries = new[]
        {
            "Freezing", "Bracing", "Chilly", "Cool", "Mild", "Warm", "Balmy", "Hot", "Sweltering", "Scorching"
        };

        private readonly ILogger<WeatherForecastController> _logger;

        public WeatherForecastController(ILogger<WeatherForecastController> logger)
        {
            _logger = logger;
        }

        private readonly ProducerConfig config = new ProducerConfig();
     

        [HttpGet(Name = "GetWeatherForecast")]
        public IEnumerable<WeatherForecast> Get()
        {
            return Enumerable.Range(1, 5).Select(index => new WeatherForecast
            {
                Date = DateTime.Now.AddDays(index),
                TemperatureC = Random.Shared.Next(-20, 55),
                Summary = Summaries[Random.Shared.Next(Summaries.Length)]
            })
            .ToArray();
        }

        [HttpPost]
        public async Task<ActionResult> PostAsync([FromBody] OrderRequest value)
        {
            config.BootstrapServers = "localhost:9092";
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }

            //Serialize 
            string serializedOrder = JsonConvert.SerializeObject(value);

            var producer = new ProducerWrapper(this.config, "welcometopic5");
            await producer.writeMessage(serializedOrder);

            return Created("TransactionId", "Your Trasnaction is in progress");
        }
    }
}