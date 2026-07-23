//! Deterministic daemon configuration to concrete planner request expansion.

use marketfeed_adapter_api::{
    CandleInterval, Channel, ConcreteSubscription, ConcreteSubscriptionSet, DeliveryOptions,
};
use marketfeed_model::CatalogView;

use crate::config::{ConfigError, VenueConfig};

fn configured_channel(name: &str) -> Result<(Channel, DeliveryOptions), ConfigError> {
    let normalized = name.trim().to_ascii_lowercase();
    let channel = match normalized.as_str() {
        "trades" => Channel::Trades,
        "quote" | "ticker" => Channel::Quote,
        "l2" | "l2_book" | "book" => Channel::L2Book {
            depth: None,
            cadence: None,
        },
        "funding" => Channel::Funding,
        "open_interest" => Channel::OpenInterest,
        "liquidations" => Channel::Liquidations,
        "mark" | "mark_price" => Channel::MarkPrice,
        "index" => Channel::IndexPrice,
        "candles" => Channel::Candles {
            interval: CandleInterval::M1,
        },
        _ => {
            return Err(ConfigError::Validation(format!(
                "unknown configured channel {name:?}"
            )));
        }
    };
    let delivery = match channel {
        Channel::Quote => DeliveryOptions {
            emit_bbo: true,
            ..DeliveryOptions::default()
        },
        Channel::L2Book { .. } => DeliveryOptions {
            emit_book_snapshots: true,
            emit_book_deltas: true,
            emit_bbo: true,
        },
        _ => DeliveryOptions::default(),
    };
    Ok((channel, delivery))
}

/// Expand one validated daemon venue into deterministic catalog-backed planner input.
///
/// The configured symbol order is preserved, then the configured channel order.
/// A configured symbol that is absent from the catalog is rejected instead of
/// silently falling back to an adapter default.
pub(crate) fn expand_concrete_subscriptions(
    venue: &VenueConfig,
    catalog: &CatalogView,
) -> Result<ConcreteSubscriptionSet, ConfigError> {
    let mut channels = Vec::with_capacity(venue.channels.len());
    for name in &venue.channels {
        channels.push(configured_channel(name)?);
    }

    let mut items = Vec::with_capacity(venue.symbols.len().saturating_mul(channels.len()));
    for symbol in &venue.symbols {
        let instrument = catalog.find_by_native(symbol).ok_or_else(|| {
            ConfigError::Validation(format!(
                "venue {}: configured symbol {symbol:?} missing from catalog",
                venue.id
            ))
        })?;
        for (channel, delivery) in &channels {
            items.push(ConcreteSubscription {
                instrument: instrument.id,
                channel: channel.clone(),
                delivery: *delivery,
            });
        }
    }
    Ok(ConcreteSubscriptionSet { items })
}

#[cfg(test)]
mod tests {
    use marketfeed_adapter_api::{CandleInterval, Channel, DeliveryOptions};
    use marketfeed_model::{InstrumentKind, VenueId};

    use crate::config::DaemonConfig;
    use crate::run::catalog_for_venue;

    use super::expand_concrete_subscriptions;

    fn configured_venue(symbols: &[&str], channels: &[&str]) -> crate::config::VenueConfig {
        let symbols = symbols
            .iter()
            .map(|symbol| format!(r#""{symbol}""#))
            .collect::<Vec<_>>()
            .join(", ");
        let channels = channels
            .iter()
            .map(|channel| format!(r#""{channel}""#))
            .collect::<Vec<_>>()
            .join(", ");
        let config = DaemonConfig::from_toml_str(&format!(
            r#"
            [telemetry]
            bind = "127.0.0.1:0"

            [[venues]]
            id = "binance-spot"
            adapter = "binance"
            symbols = [{symbols}]
            channels = [{channels}]
            "#
        ))
        .expect("valid fixture config");
        config.venues.into_iter().next().expect("venue")
    }

    #[test]
    fn configured_symbols_and_channels_expand_to_concrete_set() {
        let venue = configured_venue(&["ETHUSDT", "BTCUSDT"], &["trades", "l2"]);
        let catalog = catalog_for_venue(
            VenueId(2),
            "binance-spot",
            InstrumentKind::Spot,
            &venue.symbols,
        );

        let request = expand_concrete_subscriptions(&venue, &catalog).expect("expand");

        assert_eq!(request.items.len(), 4);
        assert_eq!(request.items[0].instrument.0, 1);
        assert_eq!(request.items[0].channel, Channel::Trades);
        assert_eq!(request.items[1].instrument.0, 1);
        assert_eq!(
            request.items[1].channel,
            Channel::L2Book {
                depth: None,
                cadence: None,
            }
        );
        assert_eq!(request.items[2].instrument.0, 2);
        assert_eq!(request.items[2].channel, Channel::Trades);
        assert_eq!(request.items[3].instrument.0, 2);
        assert_eq!(
            request.items[3].channel,
            Channel::L2Book {
                depth: None,
                cadence: None,
            }
        );
    }

    #[test]
    fn unknown_symbol_is_rejected() {
        let venue = configured_venue(&["ETHUSDT"], &["trades"]);
        let catalog = catalog_for_venue(
            VenueId(2),
            "binance-spot",
            InstrumentKind::Spot,
            &["BTCUSDT".to_string()],
        );

        let error = expand_concrete_subscriptions(&venue, &catalog).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("venue binance-spot: configured symbol \"ETHUSDT\" missing from catalog"),
            "{error}"
        );
    }

    #[test]
    fn configured_channel_aliases_expand_to_expected_requests() {
        let quote_delivery = DeliveryOptions {
            emit_bbo: true,
            ..DeliveryOptions::default()
        };
        let l2_delivery = DeliveryOptions {
            emit_book_snapshots: true,
            emit_book_deltas: true,
            emit_bbo: true,
        };
        let cases = [
            ("trades", Channel::Trades, DeliveryOptions::default()),
            ("quote", Channel::Quote, quote_delivery),
            ("ticker", Channel::Quote, quote_delivery),
            (
                "l2",
                Channel::L2Book {
                    depth: None,
                    cadence: None,
                },
                l2_delivery,
            ),
            (
                "l2_book",
                Channel::L2Book {
                    depth: None,
                    cadence: None,
                },
                l2_delivery,
            ),
            (
                "book",
                Channel::L2Book {
                    depth: None,
                    cadence: None,
                },
                l2_delivery,
            ),
            ("funding", Channel::Funding, DeliveryOptions::default()),
            (
                "open_interest",
                Channel::OpenInterest,
                DeliveryOptions::default(),
            ),
            (
                "liquidations",
                Channel::Liquidations,
                DeliveryOptions::default(),
            ),
            ("mark", Channel::MarkPrice, DeliveryOptions::default()),
            ("mark_price", Channel::MarkPrice, DeliveryOptions::default()),
            ("index", Channel::IndexPrice, DeliveryOptions::default()),
            (
                "candles",
                Channel::Candles {
                    interval: CandleInterval::M1,
                },
                DeliveryOptions::default(),
            ),
        ];

        for (configured, expected_channel, expected_delivery) in cases {
            assert_eq!(
                super::configured_channel(configured),
                Ok((expected_channel, expected_delivery)),
                "{configured}"
            );
        }
    }
}
