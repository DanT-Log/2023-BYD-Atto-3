// Gates the whole dashboard behind HTTP Basic Auth. Vercel's native
// Password Protection needs a paid Pro add-on ($150/mo) and isn't
// available on Hobby, so this is the documented free workaround:
// https://vercel.com/kb/guide/how-do-i-add-password-protection-to-my-vercel-deployment
//
// The actual username/password live in Vercel's project Environment
// Variables (Settings -> Environment Variables), never in this file or
// the repo -- important since this repo is public.

export const config = {
  matcher: '/((?!favicon.ico).*)',
};

export default function middleware(request) {
  const authHeader = request.headers.get('authorization');

  if (authHeader) {
    const [scheme, encoded] = authHeader.split(' ');
    if (scheme === 'Basic' && encoded) {
      const decoded = atob(encoded);
      const separatorIndex = decoded.indexOf(':');
      const user = decoded.slice(0, separatorIndex);
      const pass = decoded.slice(separatorIndex + 1);

      if (user === process.env.BASIC_AUTH_USER && pass === process.env.BASIC_AUTH_PASSWORD) {
        return; // credentials correct, let the request through
      }
    }
  }

  return new Response('Authentication required', {
    status: 401,
    headers: { 'WWW-Authenticate': 'Basic realm="Atto 3 Dashboard"' },
  });
}
