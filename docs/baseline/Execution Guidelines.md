# **SmartCourse — Execution Guidelines**

[SmartCourse — Execution Guidelines](#smartcourse-—-execution-guidelines)

[1\. Objective of This Assignment](#1.-objective-of-this-assignment)

[2\. Execution Approach](#2.-execution-approach)

[Module-Based Progression](#module-based-progression)

[Weekly Milestone Flow](#weekly-milestone-flow)

[3\. Submission Guidelines](#3.-submission-guidelines)

[GitHub Repository](#github-repository)

[README](#readme)

[Technical Documentation](#technical-documentation)

[4\. Learning Expectations](#4.-learning-expectations)

[5\. Execution Plan (3 Weeks)](#5.-execution-plan-\(3-weeks\))

[Focus: Core Platform \+ Distributed Systems](#focus:-core-platform-+-distributed-systems)

[🟢 Week 1 — Foundation \+ Core Services](#🟢-week-1-—-foundation-+-core-services)

[Scope](#scope)

[Deliverables](#deliverables)

[🟡 Week 2 — Enrollment \+ Publishing Workflow](#🟡-week-2-—-enrollment-+-publishing-workflow)

[Scope](#scope-1)

[Deliverables](#deliverables-1)

[🔴 Week 3 — Event-Driven System \+ Observability](#🔴-week-3-—-event-driven-system-+-observability)

[Scope](#scope-2)

[Deliverables](#deliverables-2)

[Part A — Weekly Tracking Table](#weekly-tracking-table)

[6\. Final Deliverables Checklist](#6.-final-deliverables-checklist)

[7\. Evaluation Approach](#7.-evaluation-approach)

# **1\. Objective of This Assignment**

This assignment is designed as a **structured learning journey** for engineers. The intention is not just to complete features, but to:

* Build a strong understanding of **distributed systems and scalable architecture**  
* Gain hands-on experience with **event-driven design and workflows**  
* Learn how to design for **reliability, consistency, and observability**  
* Develop confidence working with the **defined tech stack**  
* Apply **design principles and real-world engineering practices**

Think of this as an opportunity to **learn by building**, not just delivering.

---

# **2\. Execution Approach**

## **Module-Based Progression**

The assignment is structured in **weekly modules**.

To get the most out of this learning process:

* Try to **complete each module within the assigned week**  
* Share your progress with your mentor for review  
* Address feedback before moving ahead

This helps ensure:

* Concepts are well understood  
* Foundations are strong before building further

## **Weekly Milestone Flow**

Each week should ideally follow this cycle:

1. Work on the assigned milestone  
2. Submit your work for mentor review  
3. Discuss feedback and suggestions  
4. Refine your implementation  
5. Move to the next module

This is meant to be a **learning loop**, not just a checklist.

---

# **3\. Submission Guidelines**

## **GitHub Repository**

Maintain a clean and structured repository:

* Organized project structure  
* Clear module/service separation  
* Meaningful commits

## **README**

Your README should help someone quickly understand your system:

* Project overview  
* Architecture diagram  
* Setup instructions  
* API overview  
* Tech stack used

## **Technical Documentation**

Include a separate document covering:

* Service/module breakdown  
* Data flow and event flow  
* Key design decisions  
* Assumptions and tradeoffs

---

# **4\. Learning Expectations**

This assignment is most valuable when you focus on **understanding the “why” behind decisions**.

Try to build clarity on:

* Event-driven architecture  
* Idempotency and consistency  
* Distributed workflows (Temporal)  
* Async processing (Kafka, Asynq (Redis-based) or RabbitMQ + native Go workers.)


---

# **5\. Execution Plan (3 Weeks)**

## **Focus: Core Platform \+ Distributed Systems**

This part focuses on building a **reliable and scalable backend foundation** for SmartCourse.

## **🟢 Week 1 — Foundation \+ Core Services**

### **Scope**

* Initial system design and setup  
* User & Course management (basic CRUD)  
* Role handling (student/instructor)

### **Deliverables**

* Service structure defined  
* Database schema created  
* Basic APIs working  
* Local setup ready

## **🟡 Week 2 — Enrollment \+ Publishing Workflow**

### **Scope**

* Enrollment system with:  
  * Duplicate handling  
  * Basic validations  
* Course publishing workflow using Temporal  
* Content structure (modules/lessons)

### **Deliverables**

* Enrollment flow working reliably  
* Workflow orchestration integrated  
* Course state transitions handled

## **🔴 Week 3 — Event-Driven System \+ Observability**

### **Scope**

* Kafka integration for events  
* Background workers (Asynq (Redis-based) or RabbitMQ + native Go workers.)  
* Basic analytics metrics  
* Observability setup (logs, tracing, metrics)

### **Deliverables**

* Event-driven flows working  
* Analytics pipeline initialized  
* Basic monitoring and tracing available

## **Weekly Tracking Table**

| Week | Focus Area | What to Aim For | Done |
| ----- | ----- | ----- | ----- |
| Week 1 | Foundation & Core Services | Working APIs, DB design, system structure |  |
| Week 2 | Enrollment & Workflows | Stable enrollment \+ publishing workflow |  |
| Week 3 | Events & Observability | Async processing \+ visibility into system |  |

# **6\. Final Deliverables Checklist**

Before wrapping up, try to ensure:

- [ ] All modules are completed and reviewed  
- [ ] Codebase is clean and well-structured  
- [ ] README and documentation are complete  
- [ ] Key workflows are working end-to-end  
- [ ] You are comfortable explaining your design

---

# **7\. Evaluation Approach**

Evaluation will be based on:

* Clarity of architecture  
* Code quality and structure  
* Use of the tech stack  
* Handling of edge cases and failures  
* Observability and system thinking  
* Depth of understanding