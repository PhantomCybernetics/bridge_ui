const socket = io("https://mrkbk.local:1337", {
    path:'/app/socket.io/',
    auth: {
        id_app: '6476b0cb2a6d250ce840ad5e',
        key: '6476b0cb2a6d250ce840ad5d'
    },
    autoConnect: false
});

let config = {
    sdpSemantics: 'unified-plan',
    iceServers: [{urls: ['stun:stun.l.google.com:19302']}]
};

let supported_msg_types = null; //fetched static

let pc = new RTCPeerConnection(config);
let pc_connected = false;

let panels = {}
let topics = {} // str id => { msg_types: str[]}
let topic_dcs = {}; //str id => RTCDataChannel

function FindMessageType(search, msg_types) {
    for (let i = 0; i < msg_types.length; i++) {
        if (msg_types[i].name == search) {
            return msg_types[i];
        }
    }
    return null;
}

function buf2hex(buffer) { // buffer is an ArrayBuffer
    return [...new Uint8Array(buffer)]
        .map(x => x.toString(16).padStart(2, '0'))
        .join(' ');
}

function escapeHtml(unsafe)
{
    return unsafe
         .replace(/&/g, "&amp;")
         .replace(/</g, "&lt;")
         .replace(/>/g, "&gt;")
         .replace(/"/g, "&quot;")
         .replace(/'/g, "&#039;");
 }

 function nl2br (str, is_xhtml) {
    if (typeof str === 'undefined' || str === null) {
        return '';
    }
    var breakTag = (is_xhtml || typeof is_xhtml === 'undefined') ? '<br />' : '<br>';
    return (str + '').replace(/([^>\r\n]?)(\r\n|\n\r|\r|\n)/g, '$1' + breakTag + '$2');
}

var prettifyXml = function(sourceXml)
{
    var xmlDoc = new DOMParser().parseFromString(sourceXml, 'application/xml');
    var xsltDoc = new DOMParser().parseFromString([
        // describes how we want to modify the XML - indent everything
        '<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform">',
        '  <xsl:strip-space elements="*"/>',
        '  <xsl:template match="para[content-style][not(text())]">', // change to just text() to strip space in text nodes
        '    <xsl:value-of select="normalize-space(.)"/>',
        '  </xsl:template>',
        '  <xsl:template match="node()|@*">',
        '    <xsl:copy><xsl:apply-templates select="node()|@*"/></xsl:copy>',
        '  </xsl:template>',
        '  <xsl:output indent="yes"/>',
        '</xsl:stylesheet>',
    ].join('\n'), 'application/xml');

    var xsltProcessor = new XSLTProcessor();
    xsltProcessor.importStylesheet(xsltDoc);
    var resultDoc = xsltProcessor.transformToDocument(xmlDoc);
    var resultXml = new XMLSerializer().serializeToString(resultDoc);
    return resultXml;
};

function GetFile(url) {
    alert('TODO:\n'+url+'');
}

function linkifyURLs(text, is_xhtml) {
    const options = {
        //rel: 'nofollow noreferrer noopener',
        formatHref: {
          hashtag: (val) => `https://www.twitter.com/hashtag/${val.substr(1)}`,
          mention: (val) => `https://github.com/${val.substr(1)}`
        },
        render: ({ tagName, attributes, content }) => {
            let attrs = "";
            tagName = 'A';
            for (const attr in attributes) {
                if (attr == 'href') {
                    attrs += ` ${attr}=javascript:GetFile(\'${attributes[attr]}\');`;
                } else
                    attrs += ` ${attr}=${attributes[attr]}`;
            }
            return `<${tagName}${attrs}>${content}</${tagName}>`;
        },
      }

      if (is_xhtml)
        return linkifyHtml(text, options)
    else
        return linkifyStr(text, options)
}

function SetWebRTCSatusLabel() {

    let state = null;
    if (pc)
        state = pc.connectionState

    if (state != null)
        state = state.charAt(0).toUpperCase() + state.slice(1);
    else
        state = 'n/a'

    if (state == 'Connected')
        $('#webrtc_status').html('<span class="online">'+state+'</span>');
    else
        $('#webrtc_status').html('<span class="offline">'+state+'</span>');
}

function SetSocketIOSatusLabel() {
    let state = 'n/a';
    if (socket)
        state = socket.connected ? 'Connected' : 'Disconnected';

    if (state == 'Connected')
        $('#socketio_status').html('<span class="online">'+state+'</span>');
    else
        $('#socketio_status').html('<span class="offline">'+state+'</span>');
}

function lerpColor(a, b, amount) {

    var ah = parseInt(a.replace(/#/g, ''), 16),
        ar = ah >> 16, ag = ah >> 8 & 0xff, ab = ah & 0xff,
        bh = parseInt(b.replace(/#/g, ''), 16),
        br = bh >> 16, bg = bh >> 8 & 0xff, bb = bh & 0xff,
        rr = ar + amount * (br - ar),
        rg = ag + amount * (bg - ag),
        rb = ab + amount * (bb - ab);

    return '#' + ((1 << 24) + (rr << 16) + (rg << 8) + rb | 0).toString(16).slice(1);
}

function ProcessRobotData(robot_data) {
    if (robot_data['err']) {
        $('#robot_info').html('Error connecting to robot...');
        return;
    }

    console.log('SIO got robot data', robot_data);

    if (robot_data['name'])
        $('#robot_name').html(robot_data['name']);

    console.log('Robot data: ', robot_data);

    let robot_online = robot_data['ip'] ? true : false;

    if (robot_online && (!pc || pc.connectionState != 'connected')) {
        WebRTC_Negotiate(robot_data['id_robot']);
    }

    $('#robot_info').html('ID: '+ robot_data['id_robot']
                            + ' @ '
                            + (robot_online ? '<span class="online">'+robot_data['ip'].replace('::ffff:', '')+'</span>':'<span class="offline">Offline</span>')+' '
                            + 'WebRTC: <span id="webrtc_status"></span> '
                            + 'Socket.io: <span id="socketio_status"></span>'
                            );

    SetWebRTCSatusLabel();
    SetSocketIOSatusLabel();
}

function WebRTC_Negotiate(id_robot)
{
    console.log('WebRTC negotiating... ');

    return pc.createOffer().then(function(offer) {
        return pc.setLocalDescription(offer);
    }).then(function() {
        // wait for ICE gathering to complete
        return new Promise(function(resolve) {
            if (pc.iceGatheringState === 'complete') {
                resolve();
            } else {
                function checkState() {
                    if (pc.iceGatheringState === 'complete') {
                        pc.removeEventListener('icegatheringstatechange', checkState);
                        resolve();
                    }
                }
                pc.addEventListener('icegatheringstatechange', checkState);
            }
        });
    }).then(function() {
        let offer = pc.localDescription;
        console.log('ICE gathering done, sending local offer: ', offer)
        socket.emit('offer', { 'id_robot': id_robot, 'sdp': offer.sdp, 'type': offer.type}, (answer) => {
            if (answer.err) {
                console.error('Offer returned error', answer);
                return;
            }
            console.log('Setting remote answer:', answer);
            return pc.setRemoteDescription(answer);
        });
    });

}



function ToggleReadTopicSubscription(id_robot, topic, state) {
    console.warn((state ? 'Subscribing to read ' : 'Unsubscribing from reading ') + topic);

    let subscription_data = {
        id_robot: id_robot,
        topics: [ [ topic, state ? 1 : 0 ] ]
    };

    // toggle read subscription
    socket.emit('subcribe:read', subscription_data, (res) => {

        if (res['success']) {
            for (let i = 0; i < res['subscribed'].length; i++) {

                let topic = res['subscribed'][i][0];
                let id = res['subscribed'][i][1];

                if (topic_dcs[topic]) {
                    console.warn('Closing local read DC '+topic);
                    topic_dcs[topic].close();
                    delete topic_dcs[topic];
                }

                console.log('Opening local read DC '+topic+' id='+id)
                let dc = pc.createDataChannel(topic, {
                    negotiated: true,
                    id:id
                });

                topic_dcs[topic] = dc;

                dc.addEventListener('open', (ev)=> {
                    console.warn('DC '+topic+' open', dc)
                });
                dc.addEventListener('close', (ev)=> {
                    console.warn('DC '+topic+' close')
                    delete topic_dcs[topic];
                });
                dc.addEventListener('error', (ev)=> {
                    console.error('DC '+topic+' error', ev)
                    delete topic_dcs[topic]
                });
                dc.addEventListener('message', (ev)=> {
                    let panel = panels[topic];
                    if (!panel)
                        return;

                    if (!$('#update_panel_'+panel.n).is(':checked'))
                        return;

                    panel.OnData(ev);
                });


            }

            for (let i = 0; i < res['unsubscribed'].length; i++) {

                let topic = res['unsubscribed'][i][0];
                let id = res['unsubscribed'][i][1];

                if (topic_dcs[topic]) {
                    console.warn('Closing local read DC '+topic);
                    topic_dcs[topic].close();
                    delete topic_dcs[topic];
                }
            }
        } else {
            console.error('Read subscription err: ', res);

        }
    });
}

function TogglePanel(topic, state) {
    let panel = panels[topic];
    if (state) {
        if (!panel) {
            panel = new Panel(topic, topics[topic] ? topics[topic].msg_types : null);
            panels[topic] = panel;
        }
    } else if (panel) {
        panel.Close();
        delete panels[topic];
    }

    UpdateUrlHash();
}

function UpdateUrlHash() {
    let hash = [];
    for (let topic in panels) {
        hash.push(topic);
    }
    if (hash.length > 0)
        window.location.hash = ''+hash.join(';');
    else
        window.location.hash = '';
}