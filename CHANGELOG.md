# Changelog

## Unreleased

- El listener LAN admite HTTP con `server.lan.tls.enabled: false`; Bearer y la allowlist CIDR siguen aplicándose. Sin TLS, el tráfico y las credenciales no están cifrados y se emite una advertencia al iniciar.
- HTTPS se mantiene como predeterminado de compatibilidad cuando `tls.enabled` se omite; certificados inválidos impiden el inicio sin fallback.
