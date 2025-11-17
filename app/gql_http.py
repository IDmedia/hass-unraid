import json
import httpx
from typing import Any, Dict, Optional


class GraphQLClient:
    def __init__(self, endpoint_url: str, api_key: str, verify_ssl: bool = True, timeout: int = 30, logger=None):
        self.endpoint_url = endpoint_url.rstrip('/')
        self.api_key = api_key
        self.verify_ssl = verify_ssl
        self.timeout = timeout
        self.logger = logger
        self._client = httpx.AsyncClient(
            verify=self.verify_ssl,
            timeout=self.timeout,
            headers={
                'Content-Type': 'application/json',
                'x-api-key': self.api_key,
            },
        )

    async def query(self, query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        payload = {'query': query}
        if variables:
            payload['variables'] = variables

        r = await self._client.post(self.endpoint_url, content=json.dumps(payload))
        if r.status_code >= 400:
            if self.logger:
                self.logger.error(f'GraphQL HTTP {r.status_code}: {r.text}')
            raise httpx.HTTPStatusError(
                f'GraphQL HTTP {r.status_code}: {r.text}',
                request=r.request,
                response=r,
            )

        data = r.json()
        if 'errors' in data and data['errors']:
            if self.logger:
                self.logger.error(f'GraphQL errors: {data["errors"]}')
            raise RuntimeError(f'GraphQL errors: {data["errors"]}')
        return data.get('data', {})

    async def get_csrf_token(self) -> Optional[str]:
        q = """
        query Vars {
          vars {
            csrfToken
          }
        }
        """
        data = await self.query(q)
        vars_obj = data.get('vars') or {}
        return vars_obj.get('csrfToken')

    async def get_version(self) -> Optional[str]:
        q = """
        query Vars {
          vars {
            version
          }
        }
        """
        data = await self.query(q)
        vars_obj = data.get('vars') or {}
        v = vars_obj.get('version')
        return str(v) if v else None

    async def aclose(self):
        await self._client.aclose()
