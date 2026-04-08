# Using GitHub apps for authenticating from eclccserver

## Creating and Configuring a GitHub App for eclccserver

Here is a step-by-step guide to creating and configuring a GitHub App to use with the authentication mechanism.

### Step 1: Create the GitHub App
1. Go to your GitHub account (or Organization) **Settings**.
2. On the left sidebar, scroll down and click **Developer settings**.
3. Select **GitHub Apps** in the left sidebar, then click the **New GitHub App** button.
4. Fill out the **Register new GitHub App** form:
   * **GitHub App name**: Give it a recognizable name (e.g., `HPCC ECL Fetcher`).
   * **Homepage URL**: You can use your organization's URL or the HPCC platform repository URL (this is just required by the form).
   * **Webhook**: Uncheck the **Active** checkbox. (HPCC only needs to pull code; it doesn't need to listen for GitHub events).
5. Set the **Repository permissions**:
   * Under **Repository permissions**, find **Contents** and set the access to **Read-only**. (This gives the app permission to perform `git fetch` / `git clone`).
6. Scroll to the bottom (**Where can this GitHub App be installed?**):
   * Select **Any account** if you plan to fetch repositories owned by different organizations/users. Select **Only on this account** if you exclusively fetch repositories within your current organization.
7. Click **Create GitHub App**.

### Step 2: Gather Your Secrets
Immediately after creating the app, you will land on its settings page. You need to collect three items to populate your HPCC secrets configuration:

1. **The App ID** (`appid`):
   * Look at the "About" section at the top of the page. Copy the **App ID**.
2. **The Private Key** (`appkey`):
   * Scroll down to the **Private keys** section.
   * Click **Generate a private key**.
   * This will download a `.pem` file to your computer. The entire contents of this file (including the `-----BEGIN RSA PRIVATE KEY-----` and end tags) will be your `appkey` secret.

### Step 3: Install the App & Get the Installation ID
The App ID and Private Key alone are not enough; the app must be *installed* on the target account/repository to generate the `installationid`.

1. While still on the App's settings page, click **Install App** on the left sidebar.
2. Click **Install** next to the user or organization account where the target ECL repositories live.
3. Choose whether to install it on **All repositories** or **Only select repositories**, then click **Install**.
4. You will be redirected to the installation settings page. Look at the URL in your browser's address bar. It will look like this:
   `https://github.com/settings/installations/12345678`
   or
   `https://github.com/organizations/YOUR_ORG/settings/installations/12345678`
5. Copy the numeric value at the end of the URL (e.g., `12345678`). This is your **Installation ID** (`installationid`).

## Configuring the secrets for eclccserver

1. configure the git user name to be x-access-token

2. Create a secret category "git", secret "x-access-token"

3. Place these three values highlighted above (`appid`, `appkey`, and `installationid`) into your HPCC secret.
