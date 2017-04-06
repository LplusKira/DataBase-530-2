/Users/kejunliu/.pyenv/versions/2.7/bin/scons
/Users/kejunliu/Documents/DataBase-530-2/A6_2/Build


vecLeft
vecRight

if both vec empty {
	check both acceptance
	check finalPred
	if smaller->next
	if equal {
		add both to vec respectively
		both->next continue
	}
} else {
	get nextState for both
	if both stay still {
		merge and clear the vec
	}
}


nextState() {
	if (rec = vec.front) {
		check acceptance
		//check finalPred with the other vec's front rec
		add to vec
		next
	} else {
		stay still
	}
}
//////////////////////////////////
checkSingleAcceptance {
	1: no more record
	2: not accecpted
	3: accpected
}
checkBothAcceptance {
	1: no more record
	2: not accecpted
	3: accpected
}
if has more records, this record has been obtained;
nextState {
	1: no more record
	2: not accecpted
	3: should stay still
}
//do not check finalPred in this run
vecLeft
vecRight

while (true) {
	iter->getCurrent()
	iter->getCurrent()
	if both vec empty {
		for both left and right, do:
		if (checkSingleAcceptance == 1) {
			// no more records
			break;
		} else if (==2) {
			//not accepted
			continue;
		}
		if (whichOne is smaller){
			if (!advance()) {
				break;
			} else {
				continue;
			}
		}

		if equal {
			// add both to vec respectively
			// both->next 
			vec.pushback(recl);
			vec.pushback(recr);
			for both left and right, do:
				if (!advance()) {
					break;
				}
		}
	} else {
		int stillNum = 0;
		for both left and right, do:
			if (nextState == 1){
				break;
			} else if (=2) {
				continue;
			} else if (=3) {
				stillNum ++;
			}

		if (stillNum == 2) {
			merge 
			clear the vec
		}
	}
}



nextState() {
	if (rec = vec.front) {
		if (checkSingleAcceptance == 1) {
			// no more records
			return 1;
		} else if (==2) {
			//not accepted
			return 2;
		}

		// add to vec
		// next
		vec.push_back(rec)
		if (!advance()) {
			return 1;
		}
	} else {
		return 3;
	}
}