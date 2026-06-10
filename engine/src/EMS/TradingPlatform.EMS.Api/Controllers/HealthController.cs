using Microsoft.AspNetCore.Mvc;

namespace TradingPlatform.EMS.Api.Controllers;

/// <summary>Liveness probe endpoint for Docker/Kubernetes.</summary>
[ApiController]
[Route("health")]
public sealed class HealthController : ControllerBase
{
    /// <summary>Returns 200 OK when the service is running.</summary>
    [HttpGet]
    public IActionResult Get() => Ok(new { status = "healthy" });
}
