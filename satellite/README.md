# Satellite webhook examples

Templates for the [Automated Remediation](../docs/optional/automated-remediation.md) procedure. This directory does not install Satellite.

| File | Event | `stage` |
| --- | --- | --- |
| [webhook-repository-sync.json.erb](webhook-repository-sync.json.erb) | `actions.katello.repository.sync_succeeded` | `repository_sync` |
| [webhook-content-view-promote.json.erb](webhook-content-view-promote.json.erb) | `actions.katello.content_view.promote_succeeded` | `content_view_promoted` |
| [change-approved.example.json](change-approved.example.json) | Posted by the change system, not by Satellite | `change_approved` |

`katello_errata_install_succeeded` is an audit event. Do not add a webhook that starts another install.

Create the templates and webhooks in the Satellite web UI, or with Hammer, using the steps in the guide. Target URL:

```text
https://<telemetry-bridge-route-host>/topics/security-advisories
```

`Content-Type` is `application/vnd.kafka.json.v2+json`. Enable SSL verification and trust the OpenShift ingress CA on Satellite. The HTTP Bridge does not check a shared secret.
