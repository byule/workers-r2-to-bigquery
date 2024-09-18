export async function generateJWT(serviceToken) {
    const iat = Math.floor(Date.now() / 1000);
    const exp = iat + 3600; // Token expires in 1 hour
  
    const payload = {
      iss: serviceToken.client_email,
      scope: 'https://www.googleapis.com/auth/bigquery',
      aud: 'https://oauth2.googleapis.com/token',
      exp: exp,
      iat: iat
    };
  
    const headerString = JSON.stringify({ alg: 'RS256', typ: 'JWT' });
    const payloadString = JSON.stringify(payload);
  
    const encodedHeader = btoa(headerString).replace(/=/g, '').replace(/\+/g, '-').replace(/\//g, '_');
    const encodedPayload = btoa(payloadString).replace(/=/g, '').replace(/\+/g, '-').replace(/\//g, '_');
  
    const signInput = encodedHeader + '.' + encodedPayload;
    const signature = await crypto.subtle.sign(
      { name: 'RSASSA-PKCS1-v1_5' },
      await crypto.subtle.importKey(
        'pkcs8',
        pemToArrayBuffer(serviceToken.private_key),
        { name: 'RSASSA-PKCS1-v1_5', hash: 'SHA-256' },
        false,
        ['sign']
      ),
      new TextEncoder().encode(signInput)
    );
  
    const encodedSignature = btoa(String.fromCharCode(...new Uint8Array(signature)))
      .replace(/=/g, '')
      .replace(/\+/g, '-')
      .replace(/\//g, '_');
  
    const jwt = `${encodedHeader}.${encodedPayload}.${encodedSignature}`;
  
    const tokenResponse = await fetch('https://oauth2.googleapis.com/token', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
      },
      body: new URLSearchParams({
        grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer',
        assertion: jwt,
      }),
    });
  
    const tokenData = await tokenResponse.json();
    return tokenData.access_token;
  }
  
  function pemToArrayBuffer(pem) {
    const pemContents = pem.replace(/-----BEGIN PRIVATE KEY-----/, '')
      .replace(/-----END PRIVATE KEY-----/, '')
      .replace(/\s/g, '');
    const binary = atob(pemContents);
    const buffer = new ArrayBuffer(binary.length);
    const bytes = new Uint8Array(buffer);
    for (let i = 0; i < binary.length; i++) {
      bytes[i] = binary.charCodeAt(i);
    }
    return buffer;
  }