# Dataspool Licensing Model

## Overview

Dataspool uses a **feature-based licensing model** to monetize premium capabilities while keeping the core document migration功能免费开源（AGPL-3.0）。

## License Tiers

### 🆓 Community (Free & Open Source)
**License**: AGPL-3.0  
**Price**: Free  
**Features**:
- Document database migration (Elasticsearch, OpenSearch)
- Basic transformations
- Unlimited tasks and nodes
- Community forum support

**Use Case**: Individual developers, small teams, open-source projects

---

### 💼 Basic
**Price**: $299/month or $2,990/year (perpetual: $9,900)  
**Includes Community +**:
- Relational database migration (MySQL, PostgreSQL, SQLite)
- Email support (48h SLA)
- Max 3 nodes

**Use Case**: Startups, small businesses with database migration needs

---

### 🚀 Standard
**Price**: $999/month or $9,990/year (perpetual: $29,900)  
**Includes Basic +**:
- Streaming platform integration (Kafka, Pulsar, AutoMQ)
- Priority email support (24h SLA)
- Max 10 nodes

**Use Case**: Growing companies with real-time data pipelines

---

### 💎 Professional
**Price**: $2,999/month or $29,990/year (perpetual: $89,900)  
**Includes Standard +**:
- NoSQL database migration (MongoDB, Redis, Cassandra)
- Object storage operations (S3, NFS, Parquet)
- Local shard reader (zero-copy ES migration)
- Advanced transforms (scripting, complex mappings)
- Priority phone/Slack support (8h SLA)
- Max 50 nodes

**Use Case**: Enterprises with complex multi-database environments

---

### 🏢 Enterprise
**Price**: Custom (starts at $9,999/month)  
**Includes Professional +**:
- Multi-tenancy & RBAC
- High availability clustering (unlimited nodes)
- Priority 24/7 support (1h SLA)
- Dedicated CSM & solution architect
- Custom feature development
- On-premises deployment assistance
- Annual health checks

**Use Case**: Large enterprises, managed service providers, mission-critical deployments

---

## Feature Matrix

| Feature | Community | Basic | Standard | Professional | Enterprise |
|---------|:---------:|:-----:|:--------:|:------------:|:----------:|
| **Data Sources** |
| Elasticsearch / OpenSearch | ✅ | ✅ | ✅ | ✅ | ✅ |
| MySQL / PostgreSQL | ❌ | ✅ | ✅ | ✅ | ✅ |
| Kafka / Pulsar / AutoMQ | ❌ | ❌ | ✅ | ✅ | ✅ |
| MongoDB / Redis | ❌ | ❌ | ❌ | ✅ | ✅ |
| S3 / NFS / Parquet | ❌ | ❌ | ❌ | ✅ | ✅ |
| **Performance** |
| Local shard reader | ❌ | ❌ | ❌ | ✅ | ✅ |
| Advanced transforms | ❌ | ❌ | ❌ | ✅ | ✅ |
| **Enterprise Features** |
| Multi-tenancy & RBAC | ❌ | ❌ | ❌ | ❌ | ✅ |
| High availability (3+ nodes) | ❌ | ❌ | ❌ | ❌ | ✅ |
| **Limits** |
| Max nodes | ∞ | 3 | 10 | 50 | ∞ |
| Max tasks/day | ∞ | 500 | 2,000 | 10,000 | ∞ |
| **Support** |
| Community forum | ✅ | ✅ | ✅ | ✅ | ✅ |
| Email support | ❌ | 48h | 24h | 8h | 1h |
| Phone/Slack support | ❌ | ❌ | ❌ | ✅ | ✅ |
| 24/7 support | ❌ | ❌ | ❌ | ❌ | ✅ |
| CSM & architect | ❌ | ❌ | ❌ | ❌ | ✅ |

---

## License Enforcement

### Enforcement Points

Licenses are checked at the following points:

1. **Driver Factory**: When creating source/sink instances
   ```rust
   create_source_with_license(&config, Some(&license_manager))?;
   ```

2. **Task Submission**: Before accepting new migration tasks
   ```rust
   license_manager.check_task_limit()?;
   ```

3. **Cluster Join**: When nodes join the cluster
   ```rust
   license_manager.check_node_limit(node_count)?;
   ```

4. **API Endpoints**: Protected endpoints require feature checks
   ```http
   POST /api/v1/tasks/create
   Authorization: Bearer <token>
   X-License-Feature: streaming_migration
   ```

### License File Format

Licenses are stored as YAML files (production: encrypted + RSA-signed):

```yaml
license_id: "ENT-2026-ACME-0001"
organization: "Acme Corporation"
tier: enterprise
license_type: subscription
features:
  - document_migration
  - relational_migration
  - streaming_migration
  # ... more features
issued_at: "2026-01-01T00:00:00Z"
expires_at: "2027-01-01T00:00:00Z"
max_nodes: 10
max_tasks_per_day: null
signature: "RSA2048:base64encoded..."  # Production only
```

### Loading Licenses

```rust
use dataspool::license::LicenseManager;

let mut manager = LicenseManager::new();  // Defaults to Community

// Load from file
manager.load_from_file("license.yaml").await?;

// Check features
manager.check_feature(Feature::StreamingMigration)?;
```

### Grace Period

Expired licenses have a **7-day grace period**:
- Feature checks emit warnings but don't fail
- Telemetry notifies license server
- After 7 days, premium features are disabled

---

## Trial Licenses

### Requesting a Trial

1. Visit https://dataspool.io/trial
2. Fill out form (name, email, company, use case)
3. Receive trial license via email (valid 30 days)
4. Place `license.yaml` in `/etc/dataspool/` or `~/.config/dataspool/`

### Trial Limitations

- **Duration**: 30 days from issue date
- **Tier**: Professional (most features, except enterprise-only)
- **Limits**: 3 nodes, 100 tasks/day
- **Support**: Email only (48h SLA)
- **Renewal**: Not renewable, must purchase

---

## Perpetual vs. Subscription

| Aspect | Perpetual | Subscription |
|--------|-----------|--------------|
| **Payment** | One-time | Annual/Monthly |
| **Updates** | Major version only | All versions |
| **Support** | First year included | Always included |
| **Price** | 3.3x annual price | Pay as you go |
| **Upgrades** | Pay for major versions | Automatic |

---

## Volume Discounts

- **5-10 licenses**: 10% off
- **11-50 licenses**: 20% off
- **51+ licenses**: 30% off + custom terms

---

## Academic & Non-Profit

- **50% discount** on all paid tiers
- Requires verification (`.edu` email or 501(c)(3) status)
- Annual renewal required

---

## License Compliance

### Telemetry (Optional)

Dataspool can phone home for license validation:
- Online license check (floating licenses)
- Usage statistics (anonymized)
- Feature adoption metrics

Disable with `--no-telemetry` flag (offline licenses only).

### Audit Rights

Enterprise customers: We reserve the right to audit usage annually to ensure compliance.

---

## FAQ

**Q: What happens if my license expires?**  
A: Grace period of 7 days, then premium features are disabled. Data is never deleted.

**Q: Can I upgrade mid-term?**  
A: Yes, pay the prorated difference. Downgrades effective at renewal.

**Q: What if I exceed node limits?**  
A: Additional nodes won't join cluster. Contact sales to upgrade.

**Q: Is the Community edition truly free?**  
A: Yes, AGPL-3.0. Document migration will always be free and open-source.

**Q: Can I modify the source code?**  
A: Yes (AGPL-3.0), but derivative works must also be AGPL-3.0 and you must disclose source if running as a service.

**Q: Do you offer managed hosting?**  
A: Yes, Dataspool Cloud starts at $149/month (separate from self-hosted licenses).

---

## Contact Sales

- **Email**: sales@dataspool.io
- **Phone**: +1 (555) DATASPL
- **Slack**: #sales on dataspool.slack.com
- **Web**: https://dataspool.io/pricing

---

_Last updated: 2026-02-14_
