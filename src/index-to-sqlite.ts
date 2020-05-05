import fs = require('fs');
import readline = require("readline");
import chalk = require('chalk');
//import pull = require('pull-stream');
import sqlite = require('better-sqlite3');

//import coolerModule = require('./ssb');

import { makeLogger } from './util';
import {
    SSBMessage,
} from './types';

let main = async () => {

    let log = makeLogger(chalk.cyan('indexer'), 0);

    // set up database
    const IN_MEMORY = false;
    const dbFn = IN_MEMORY ? ':memory:' : 'db.sqlite';

    if (!IN_MEMORY && fs.existsSync(dbFn)) {
        try {
            fs.unlinkSync(dbFn);
            fs.unlinkSync(dbFn + '-journal');
        } catch (e) {
        }
    }
    const db = new sqlite(dbFn);

    //log('preparing database');
    db.prepare(`
        CREATE TABLE IF NOT EXISTS authors (
            key VARCHAR(53) PRIMARY KEY,
            name TEXT,
            description TEXT,
            image TEXT
        );
    `).run();

    db.prepare(`
        CREATE TABLE IF NOT EXISTS msgs (
            key VARCHAR(52) NOT NULL PRIMARY KEY,
            sequence INTEGER NOT NULL,
	    author VARCHAR(53) NOT NULL,
            content TEXT NOT NULL,
            timestampReceived NUMBER,
            timestampAsserted NUMBER NOT NULL
        );
    `).run();

    // set up data source
    const fileStream = fs.createReadStream('log.jsonl');
    const lines = readline.createInterface({
        input: fileStream,
        crlfDelay: Infinity
    });

    // 
    let insertMsgSt = db.prepare(`
        INSERT OR REPLACE INTO msgs (
            key,
            sequence,
            author,
            content,
            timestampReceived,
            timestampAsserted
        ) VALUES (
            ?, -- key
            ?, -- sequence
            ?, -- author
            ?, -- content
            ?, -- timestampReceived
            ? -- timestampAsserted
        );
    `);
    let getAuthorSt = db.prepare(`
        SELECT * FROM authors WHERE key = ?;
    `);
    let insertAuthorSt = db.prepare(`
        INSERT OR IGNORE INTO authors (
            key
        ) VALUES (
            ? -- key
        );
    `);

    let updateNameSt = db.prepare(`UPDATE authors SET name = ? WHERE key = ?`);
    let updateImageSt = db.prepare(`UPDATE authors SET image = ? WHERE key = ?`);
    let updateDescriptionSt = db.prepare(`UPDATE authors SET description = ? WHERE key = ?`);

    let beginSt = db.prepare(`BEGIN IMMEDIATE;`);
    let commitSt = db.prepare(`COMMIT;`);

    let ingestMsg = (msg : SSBMessage) : void => {
	if (msg.value.previous === null) {
          insertAuthorSt.run(
              msg.value.author
          );
	}

        // msgs table
        insertMsgSt.run(
            msg.key,
            msg.value.sequence,
            msg.value.author,
            JSON.stringify(msg.value.content),
            msg.timestamp,  // received
            msg.value.timestamp,  // asserted
        );


        // authors table
        if (msg.value.content.type === 'about') {
            let content = msg.value.content;

	    // Only set properties when the author is describing themselves.
            if (typeof content.about === 'string' && content.about === msg.value.author) {
                // update the row with new values
                if (typeof content.name === 'string' && content.name !== '') {
		  updateNameSt.run(content.name, msg.value.author)
                }

                if (typeof content.description === 'string' && content.description !== '') {
                    updateDescriptionSt.run(content.description, msg.value.author)
                }

                if (content.image !== null && typeof content.image === 'object' && typeof content.image.link === 'string' && content.image.link !== '') {
                    updateImageSt.run(content.image.link, msg.value.author)
                } else if (typeof content.image === 'string' && content.image !== '') {
                    updateImageSt.run(content.image, msg.value.author)
                }
            }
        }
    }

    // go
    let args = process.argv.slice(2);
    let limit = +(args[0] || '100000');
    let commitEvery = +(args[1] || '1000');

    let ii = 0;
    //let printEvery = 10000;
    //let commitEvery = 1000;
    let startTime = Date.now();
    beginSt.run();
    for await (const line of lines) {
        //if (ii % printEvery === 0) {
        //    log(`${ii} / ${limit} (${Math.round(ii/limit*100*10)/10} %)`);
        //}

        let msg = JSON.parse(line) as SSBMessage;
        ingestMsg(msg);

        ii += 1;
        if (ii >= limit) { break; }
        if (ii % commitEvery === 0) {
            commitSt.run();
            beginSt.run();
        }
    }
    commitSt.run();
    let endTime = Date.now();

    let numAuthors = db.prepare('SELECT COUNT(*) as count FROM authors;').get().count;

    let seconds = (endTime - startTime) / 1000;
    //log();
    log(`commit transaction every ${commitEvery} messages`);
    log(`${ii} total messages`);
    log(`${Math.round(seconds*10)/10} seconds`);
    log(`${Math.round(ii/seconds*10)/10} messages per second`);
    log(`${Math.round(seconds/ii*1000*1000)/1000} ms per message`);
    log(`${numAuthors} authors in table`);
}
main();

