<div align="center">
  <img src="extras/logo.png" width="250" alt="logo">
</div>


# Unraid to Home Assistant
This Docker container parses and forwards all WebSocket messages from your Unraid server to Home Assistant using the MQTT protocol. This enables you to create dashboards that provide a superior overview compared to Unraid's native capabilities.


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
    ssl: false
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
  registry.idmedia.no/idmedia/docker/hass-unraid:latest
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
    image: registry.idmedia.no/idmedia/docker/hass-unraid:latest
```

The container should now connect to your Unraid server(s) and automatically create an entry in Home Assistant. To verify navigate to Settings->Devices & Services->MQTT. If no device is created make sure to check the contains logs using `docker logs hass-unraid`.


## Lovelace

This lovelace example is a bit complex and requires these modules in Home Assistant to work properly:
 * [button-card](https://github.com/custom-cards/button-card)
 * [vertical-stack-in-card](https://github.com/ofekashery/vertical-stack-in-card)
 * [auto-entities](https://github.com/thomasloven/lovelace-auto-entities)


Please check out the `lovelace` folder. That's where I've placed two button-card templates and the main setup for showing the server named `Kaya` just like you see in the screenshot.


### Feel free to contribute, report issues, or suggest improvements! If you find this repository useful, don't forget to star it :)

<a href="https://www.paypal.com/cgi-bin/webscr?cmd=_s-xclick&hosted_button_id=JPGHGTWP33A5L">
  <img src="https://raw.githubusercontent.com/stefan-niedermann/paypal-donate-button/master/paypal-donate-button.png" alt="Donate with PayPal" />
</a>