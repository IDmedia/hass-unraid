<div align="center">
  <img src="extras/logo.png" width="250" alt="logo">
</div>


# Unraid to Home Assistant
This Docker container parses and forwards all WebSocket messages from your Unraid server to Home Assistant using the MQTT protocol. This enables you to create dashboards that provide a superior overview compared to Unraid's native capabilities.

# Features
1. Historical data is crucial! I aimed to monitor CPU, RAM, and other attributes over time. Maybe most importantly know how often and when disks spin up in order to debug the cause easier.

2. This integration enables Unraid automations via Home Assistant. You can receive notifications for disk space running low, fan failures, and more.

3. The custom view I developed greatly enhances locating disks within the array. A disk's color changes from grey (spin-down) to yellow to red if it overheats, and warnings are displayed for disks with issues. It also has a notification when disk scrubbing is running.

4. Instantly identify which shares are on which disks and how full the disk is.

5. The "network-share" view in Home Assistant is much more intuitive than Unraid's "shares" tab, in my opinion.

<div align="center">
  <img src="extras/screenshot.png" width="250" alt="screenshot">
</div>


## Prerequisites
Ensure that Home Assistant and MQTT are correctly configured before proceeding.


## Getting started

I haven't created a Unraid template as I personally utilize docker-compose. However, setting this up shouldn't pose significant challenges.

Generate a config.yaml and store it in a directory named `data`. For instance, I'll demonstrate by configuring two servers named 'Kaya' and 'Hisa'. Ensure to adjust the MQTT settings accordingly:
```
unraid:
  - name: Kaya
    host: 192.168.1.10
    port: 80
    ssl: false
    username: root
    password: PASSWORD
    scan_interval: 30

  - name: Hisa
    host: 192.168.1.20
    port: 80
    ssl: true
	ssl_verify: false
    username: root
    password: PASSWORD
    scan_interval: 30
  
mqtt:
  host: 192.168.1.100
  port: 1883
  username: USERNAME
  password: PASSWORD
```

Now we can run our container either using `docker run` or `docker-compose`.

Docker run:
```
docker run -d \
  --name hass-unraid \
  --network bridge \
  --restart always \
  -e TZ=Europe/Oslo \
  -v $(pwd)/data:/data \
  ghcr.io/idmedia/hass-unraid:latest
```

Docker-compose:
```
version: '3'

services:
  hass-unraid:
    container_name: hass-unraid
    network_mode: bridge
    restart: always
    environment:
      - TZ=Europe/Oslo
    volumes:
      - './data:/data'
    image: ghcr.io/idmedia/hass-unraid:latest
```

The container should now connect to your Unraid server(s) and automatically create an entry in Home Assistant. To verify navigate to Settings->Devices & Services->MQTT. If no device is created make sure to check the contains logs using `docker logs hass-unraid`.


## Lovelace

Please check out the `lovelace` folder. That's where I've placed two button-card templates and the main setup for showing the server named `Kaya` just like you see in the screenshot.

This lovelace example is a bit complex and requires these modules in Home Assistant to work properly:
 * [button-card](https://github.com/custom-cards/button-card)
 * [vertical-stack-in-card](https://github.com/ofekashery/vertical-stack-in-card)
 * [auto-entities](https://github.com/thomasloven/lovelace-auto-entities)
 * [card-mod](https://github.com/thomasloven/lovelace-card-mod)


Copy the button_card templates from `/lovelace/templates/` into `/config/lovelace/templates/button_card/`:
```
network_share.yaml
simple_bar.yaml
unraid_disk.yaml
```

Ensure button-card locates the templates by adding the following line to the top of your ui-lovelace.yaml file:
```
button_card_templates: !include_dir_merge_named lovelace/templates/button_card
```


### Feel free to contribute, report issues, or suggest improvements! If you find this repository useful, don't forget to star it :)

<a href="https://www.paypal.com/cgi-bin/webscr?cmd=_s-xclick&hosted_button_id=JPGHGTWP33A5L">
  <img src="https://raw.githubusercontent.com/stefan-niedermann/paypal-donate-button/master/paypal-donate-button.png" alt="Donate with PayPal" />
</a>
