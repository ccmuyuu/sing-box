# OutboundProvider

### Structure

```json
{
  "outbound_providers": [
    {
      "type": "",
      "tag": "",
      "path": "",
      "healthcheck_url": "https://www.gstatic.com/generate_204",
      "healthcheck_interval": "1m"
    }
  ]
}
```

### Fields

| Type   | Format         |
|--------|----------------|
| `http` | [HTTP](./http) |
| `file` | [File](./file) |

#### tag

The tag of the outbound provider.

#### path

==Required==

The path of the outbound provider file.

#### healthcheck_url

The url for health check of the outbound provider.

Default is `https://www.gstatic.com/generate_204`.

#### healthcheck_interval

The interval for health check of the outbound provider. `1m` will be used if empty.
