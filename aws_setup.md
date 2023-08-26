# AWS ubuntu setup
## Prereq
```
sudo apt update
sudo apt build-essential
```

## Rabbitmq
- Run the bash script: https://www.rabbitmq.com/install-debian.html#apt-quick-start-cloudsmith
- It should auto-start rabbitmq. Check the status of `rabbitmq-server`:
```
systemctl status rabbitmq-server
```

## Postgres
- Install instructions: https://www.digitalocean.com/community/tutorials/how-to-install-postgresql-on-ubuntu-20-04-quickstart
- You'll need to create a user named `dev` with a new password. Save the password securely.
- Create role `dev` and db `arcpay`. 

## `arcpay-server`
- Create an ssh key as specified: https://docs.github.com/en/authentication/connecting-to-github-with-ssh/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent?platform=linux
- Add the public key to your personal account so that you can clone the repo on ec2.
- Clone the repo, install rust, run the `source` command displayed at the end of the logs.
- Run `cargo build` inside the repo.
