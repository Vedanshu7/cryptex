namespace TradingPlatform.OMS.Application.Settings;

/// <summary>
/// Derives OmsTopicSettings' three topic names from its Region, and validates
/// that Region is a real entry in the current universe. Kept dependency-free
/// (a plain string collection, not the Universe type itself) so it's testable
/// without constructing a whole universe file or referencing Shared.
/// </summary>
public static class OmsTopicSettingsDeriver
{
    /// <summary>
    /// Mutates <paramref name="settings"/> in place: if Region is set, any
    /// topic name still at its single-region default is overwritten with the
    /// region-prefixed name. Explicit overrides (a topic name that isn't the
    /// default) are left untouched. A no-op when Region is unset.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// Region is set but isn't present in <paramref name="knownRegions"/>.
    /// </exception>
    public static void DeriveFromRegion(OmsTopicSettings settings, IReadOnlyCollection<string> knownRegions)
    {
        ArgumentNullException.ThrowIfNull(settings);
        ArgumentNullException.ThrowIfNull(knownRegions);

        if (string.IsNullOrEmpty(settings.Region))
        {
            return;
        }

        if (!knownRegions.Contains(settings.Region))
        {
            throw new InvalidOperationException(
                $"OMS__Topics__Region is set to '{settings.Region}' but that region does not " +
                $"exist in the current universe. Known regions: {string.Join(", ", knownRegions)}.");
        }

        if (settings.InputTopic == "order-requests")
        {
            settings.InputTopic = $"{settings.Region}.order-requests";
        }

        if (settings.OutputTopic == "validated-orders")
        {
            settings.OutputTopic = $"{settings.Region}.validated-orders";
        }

        if (settings.FillsTopic == "order-fills")
        {
            settings.FillsTopic = $"{settings.Region}.order-fills";
        }
    }
}
